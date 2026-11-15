from pathlib import Path
import pandas as pd
import pendulum
from airflow.sdk import dag, task

# Módulos del proyecto
from src.extract import extraer_datos
from src.extract_sisben import extract_sisben_data
from src.transform import transformar_fuentes
from src.validation import validate_prepared
from gx.validate_icbf import validations_icbf
from gx.validate_sisben import validations_sisben
from src.load import cargar

STAGING_DIR = Path("/opt/airflow/data/staging")
RAW_ICBF_PATH = Path("/opt/airflow/data/raw/DatasetCompleto.csv")

STAGING_DIR.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="reliable_ods2",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "sisben", "icbf", "reliable"],
)
def reliable_ods2():

    @task
    def start():
        print("Iniciando Pipeline ETL ICBF - Sisbén ODS 2...")

    # -------------------------------------------------------------------------
    # RAMA ICBF
    # -------------------------------------------------------------------------
    @task
    def extract_data_icbf() -> str:
        print("Extrayendo datos de ICBF...")
        df_icbf = extraer_datos(RAW_ICBF_PATH)
        out_path = STAGING_DIR / "raw_icbf_extracted.parquet"
        df_icbf.to_parquet(out_path, index=False)
        return str(out_path)

    @task
    def validate_data_extract_icbf(file_path: str) -> str:
        print("Validando extracción de ICBF con Great Expectations...")
        validations_icbf()
        print("¡Validación de extracción ICBF exitosa!")
        return file_path

    # -------------------------------------------------------------------------
    # RAMA SISBÉN (API)
    # -------------------------------------------------------------------------
    @task
    def extract_data_sisben() -> str:
        print("Extrayendo datos del API Sisbén...")
        df_sisben = extract_sisben_data()
        out_path = STAGING_DIR / "raw_sisben_extracted.parquet"
        df_sisben.to_parquet(out_path, index=False)
        return str(out_path)

    @task
    def validate_data_extract_sisben(file_path: str) -> str:
        print("Validando extracción de Sisbén con Great Expectations...")
        validations_sisben()
        print("¡Validación de extracción Sisbén exitosa!")
        return file_path

    # -------------------------------------------------------------------------
    # TRANSFORMACIÓN
    # -------------------------------------------------------------------------
    @task
    def transform_data(icbf_path: str, sisben_path: str) -> dict:
        print("Iniciando transformación y cruce de datos...")
        df_icbf = pd.read_parquet(icbf_path)
        df_sisben = pd.read_parquet(sisben_path)

        # 1. Transformación
        df_icbf_prep, df_sisben_prep = transformar_fuentes(df_icbf, df_sisben)

        # 2. Persistencia intermedia
        path_icbf_prep = STAGING_DIR / "icbf_prepared.parquet"
        path_sisben_prep = STAGING_DIR / "sisben_prepared.parquet"

        df_icbf_prep.to_parquet(path_icbf_prep, index=False)
        df_sisben_prep.to_parquet(path_sisben_prep, index=False)

        print("Transformación de fuentes completada exitosamente.")
        return {
            "icbf_prep": str(path_icbf_prep),
            "sisben_prep": str(path_sisben_prep),
        }

    @task
    def validate_data_transform(prep_paths: dict) -> dict:
        print("Iniciando compuerta 3: validación de datos preparados...")
        df_icbf = pd.read_parquet(prep_paths["icbf_prep"])
        df_sisben = pd.read_parquet(prep_paths["sisben_prep"])

        # Para validación completa de modelo e integración
        from src.dimensional_model import construir_modelo

        tablas, reporte = construir_modelo(df_icbf, df_sisben)

        validate_prepared(
            df_icbf,
            df_sisben,
            tablas,
            reporte,
            detener_en_critica=True,
        )

        print("¡Compuerta de validación de datos preparados aprobada!")
        return prep_paths

    # -------------------------------------------------------------------------
    # CARGA POSTGRESQL
    # -------------------------------------------------------------------------
  
    @task
    def load_data(prep_paths: dict):
        print("Iniciando carga al Data Warehouse PostgreSQL...")
        from src.dimensional_model import construir_modelo
        from src.load import cargar

        df_icbf = pd.read_parquet(prep_paths["icbf_prep"])
        df_sisben = pd.read_parquet(prep_paths["sisben_prep"])

        # 1. Generar el modelo dimensional
        tablas, _ = construir_modelo(df_icbf, df_sisben)

        # 2. El esquema solo se crea si el DW está vacío (primera ejecución).
        #    En las siguientes corridas la carga es DELETE + INSERT transaccional,
        #    así un dashboard conectado nunca ve las tablas borradas.
        #    Para forzar la recreación: variable de entorno DW_RECREATE_SCHEMA=true.
        import os
        from sqlalchemy import inspect
        from src.load import crear_engine

        engine = crear_engine()
        try:
            existe = inspect(engine).has_table("fact_atencion")
        finally:
            engine.dispose()
        recrear = (not existe) or os.getenv("DW_RECREATE_SCHEMA", "false").lower() == "true"
        print(f"Esquema existente: {existe} | recrear_esquema={recrear}")
        cargar(tablas, recrear_esquema=recrear)

        print("¡Carga masiva a PostgreSQL completada con éxito!")

    # -------------------------------------------------------------------------
    # Grafo de Dependencias
    # -------------------------------------------------------------------------
    start_node = start()

    # Extracción y validación
    path_icbf = extract_data_icbf()
    val_icbf = validate_data_extract_icbf(path_icbf)

    path_sisben = extract_data_sisben()
    val_sisben = validate_data_extract_sisben(path_sisben)

    # Transformación, validación final y carga
    prep_paths = transform_data(val_icbf, val_sisben)
    val_prep = validate_data_transform(prep_paths)
    load_node = load_data(val_prep)

    # Conexiones explícitas
    start_node >> [path_icbf, path_sisben]


# Instancia del DAG
reliable_ods2()