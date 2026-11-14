"""
Productor Kafka: replay simulado de fact_atencion como eventos `atencion_reportada`.

Cada evento corresponde a una fila de fact_atencion, unida con sus dimensiones.
Es una SIMULACIÓN: las filas históricas se mezclan con semilla fija y se publican
a un ritmo configurable para imitar la llegada de reportes. Cada evento lleva
simulated=true.

Uso (desde la raíz del proyecto, con el entorno virtual activo):
    python streaming/producer.py --n 300 --rate 5
    python streaming/producer.py --n 300 --rate 5 --invalidos 6    # experimento de eventos inválidos

Variables de entorno (.env, nunca versionado):
    POSTGRES_USER, POSTGRES_PASSWORD, DW_POSTGRES_DB   (ya existen en el .env)
    STREAM_PG_HOST (def. localhost), STREAM_PG_PORT (def. 5433)
    KAFKA_BOOTSTRAP (def. localhost:9094)
"""

import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Constantes del evento. El contrato y su validación viven en el consumidor
# (consumer.py); aquí solo se construyen los eventos con esa misma forma.
TOPIC = "icbf.atencion.reportada"
EVENT_TYPE = "atencion_reportada"
SCHEMA_VERSION = 1

CONSULTA = """
SELECT f.fact_key,
       m.codigo_municipio_dane,
       m.departamento,
       z.nombre_zona          AS zona,
       s.estado_atencion,
       f.total_beneficiarios
FROM fact_atencion f
JOIN dim_municipality    m ON m.municipality_key       = f.municipality_key
JOIN dim_zone            z ON z.zone_key               = f.zone_key
JOIN dim_attention_status s ON s.attention_status_key  = f.attention_status_key
"""


def cargar_hechos():
    """Lee fact_atencion + dimensiones del Data Warehouse (PostgreSQL)."""
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER", "postgres"),
        password=(os.getenv("POSTGRES_PASSWORD") or "").strip("\"'"),
        host=os.getenv("STREAM_PG_HOST", "localhost"),
        port=int(os.getenv("STREAM_PG_PORT", "5433")),
        database=os.getenv("DW_POSTGRES_DB") or os.getenv("POSTGRES_DB", "postgres"),
    )
    engine = create_engine(url)
    try:
        return pd.read_sql(CONSULTA, engine)
    finally:
        engine.dispose()


def construir_eventos(df, replay_id):
    """Convierte filas de hechos en eventos que cumplen el contrato."""
    eventos = []
    for fila in df.itertuples(index=False):
        eventos.append({
            "event_id": f"{replay_id}-{int(fila.fact_key)}",
            "event_type": EVENT_TYPE,
            "schema_version": SCHEMA_VERSION,
            "event_time": None,                      # se fija al publicar
            "simulated": True,
            "fact_key": int(fila.fact_key),
            "codigo_municipio_dane": str(fila.codigo_municipio_dane),
            "departamento": str(fila.departamento),
            "zona": str(fila.zona),
            "estado_atencion": str(fila.estado_atencion),
            "total_beneficiarios": int(fila.total_beneficiarios),
        })
    return eventos


def corromper(evento, tipo):
    """Devuelve una copia del evento con una violación deliberada del contrato."""
    malo = dict(evento)
    malo["event_id"] = evento["event_id"] + "-INV"
    if tipo == 0:
        del malo["total_beneficiarios"]                  # falta campo obligatorio
    elif tipo == 1:
        malo["codigo_municipio_dane"] = "ABC"            # formato DANE inválido
    elif tipo == 2:
        malo["total_beneficiarios"] = -5                 # fuera de rango
    else:
        malo["zona"] = "ZONA_DESCONOCIDA"                # valor fuera del dominio
    return malo


def mezclar_invalidos(eventos, cantidad, semilla):
    """Inserta `cantidad` eventos inválidos en posiciones aleatorias reproducibles."""
    rng = random.Random(semilla)
    salida = [(e, False) for e in eventos]
    for i in range(cantidad):
        base = rng.choice(eventos)
        pos = rng.randint(1, len(salida))
        salida.insert(pos, (corromper(base, i % 4), True))
    return salida


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--n", type=int, default=300, help="eventos válidos a publicar (def. 300)")
    ap.add_argument("--rate", type=float, default=2.0, help="eventos por segundo (def. 2)")
    ap.add_argument("--seed", type=int, default=42, help="semilla del muestreo (def. 42)")
    ap.add_argument("--replay-id", default=None, help="identificador del replay (def. fecha-hora UTC)")
    ap.add_argument("--invalidos", type=int, default=0, help="eventos inválidos a inyectar (def. 0)")
    args = ap.parse_args()

    if args.n < 1 or args.rate <= 0:
        sys.exit("--n debe ser >= 1 y --rate > 0")

    from confluent_kafka import Producer

    replay_id = args.replay_id or datetime.now(timezone.utc).strftime("r%Y%m%dT%H%M%S")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")

    print(f"[productor] leyendo fact_atencion del DW ...")
    hechos = cargar_hechos()
    print(f"[productor] filas en el hecho: {len(hechos):,}")
    muestra = hechos.sample(n=min(args.n, len(hechos)), random_state=args.seed)
    eventos = construir_eventos(muestra, replay_id)
    secuencia = mezclar_invalidos(eventos, args.invalidos, args.seed)
    print(f"[productor] replay_id={replay_id} | eventos válidos={len(eventos)} "
          f"| inválidos inyectados={args.invalidos} | ritmo={args.rate}/s | SIMULACIÓN")

    productor = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
    })

    por_particion = Counter()
    fallos = []
    benef_validos = 0

    def al_entregar(error, mensaje):
        if error is not None:
            fallos.append(str(error))
        else:
            por_particion[mensaje.partition()] += 1

    pausa = 1.0 / args.rate
    inicio = time.time()
    enviados = 0
    try:
        for evento, es_invalido in secuencia:
            evento = dict(evento)
            evento["event_time"] = datetime.now(timezone.utc).isoformat()
            llave = evento.get("codigo_municipio_dane", "SIN-LLAVE")
            productor.produce(
                TOPIC,
                key=str(llave).encode("utf-8"),
                value=json.dumps(evento, ensure_ascii=False).encode("utf-8"),
                on_delivery=al_entregar,
            )
            productor.poll(0)
            enviados += 1
            if not es_invalido:
                benef_validos += evento["total_beneficiarios"]
            etiqueta = "INVÁLIDO (inyectado)" if es_invalido else "ok"
            if es_invalido or enviados % 25 == 0 or enviados == 1:
                print(f"[productor] #{enviados:>5} {evento['event_id']} municipio={llave} {etiqueta}")
            time.sleep(pausa)
    except KeyboardInterrupt:
        print("\n[productor] interrumpido por el usuario; vaciando la cola ...")
    finally:
        pendientes = productor.flush(30)

    seg = time.time() - inicio
    print("\n[productor] ----- RESUMEN -----")
    print(f"  publicados:            {enviados}")
    print(f"  confirmados por Kafka: {sum(por_particion.values())}")
    print(f"  por partición:         {dict(sorted(por_particion.items()))}")
    print(f"  beneficiarios en eventos válidos: {benef_validos:,}")
    print(f"  duración: {seg:.1f} s | ritmo real: {enviados / seg:.2f} eventos/s")
    if fallos or pendientes:
        print(f"  ATENCIÓN: fallos de entrega={len(fallos)}, pendientes={pendientes}")
        if fallos:
            print(f"  primer error: {fallos[0]}")
        sys.exit(1)


if __name__ == "__main__":
    main()