import json
import os
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Consumer
import psycopg2
from dotenv import load_dotenv

# ============================================================
# CONFIGURACIÓN
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
TOPIC = "icbf.atencion.reportada"

load_dotenv()

# 1. Conexión a PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=os.getenv("DW_POSTGRES_DB", os.getenv("POSTGRES_DB", "postgres")),       
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""), 
        host=os.getenv("STREAM_PG_HOST", "localhost"),
        port=os.getenv("STREAM_PG_PORT", "5433")
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("✅ Conexión a PostgreSQL establecida.")
except Exception as e:
    print(f"❌ ERROR DETALLADO al conectar a PostgreSQL: {e}")
    raise SystemExit(1)

# 2. Configuración del Consumidor
consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"),
    "group.id": "icbf-streaming-team",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

# ============================================================
# INICIALIZACIÓN DE BASE DE DATOS
# ============================================================
def init_db():
    # 1. Tabla de Eventos Crudos
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_atencion_events (
            raw_event_id VARCHAR(100) PRIMARY KEY,
            raw_payload TEXT NOT NULL,
            received_at TIMESTAMP NOT NULL
        )
    """)
    # 2. Dimensión Ubicación
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_streaming_location (
            location_id SERIAL PRIMARY KEY,
            codigo_municipio_dane VARCHAR(50) NOT NULL,
            departamento VARCHAR(100) NOT NULL,
            zona VARCHAR(50) NOT NULL,
            UNIQUE(codigo_municipio_dane, departamento, zona)
        )
    """)
    # 3. Dimensión Estado
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_streaming_status (
            status_id SERIAL PRIMARY KEY,
            estado_atencion VARCHAR(100) UNIQUE NOT NULL
        )
    """)
    # 4. Dimensión Fecha (Simplificada para el stream)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_streaming_date (
            date_id INT PRIMARY KEY, 
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL
        )
    """)
    # 5. Tabla de Hechos Streaming
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_atencion_streaming (
            streaming_id SERIAL PRIMARY KEY,
            raw_event_id VARCHAR(100) REFERENCES raw_atencion_events(raw_event_id),
            location_id INT REFERENCES dim_streaming_location(location_id),
            status_id INT REFERENCES dim_streaming_status(status_id),
            date_id INT REFERENCES dim_streaming_date(date_id),
            total_beneficiarios INT NOT NULL,
            event_timestamp TIMESTAMP,
            processed_timestamp TIMESTAMP,
            processing_delay_seconds FLOAT
        )
    """)
    # 6. Tabla de Eventos Rechazados (DLQ)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rejected_atencion_events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(100),
            raw_event TEXT,
            rejection_reason TEXT,
            rejected_at TIMESTAMP
        )
    """)
    print("✅ Modelo Dimensional Streaming validado/creado en PostgreSQL.")

# ============================================================
# FUNCIONES DE PROCESAMIENTO
# ============================================================
def validate_event(event):
    """Valida los campos del evento de acuerdo a las corrupciones inyectadas."""
    required_fields = [
        "event_id", "codigo_municipio_dane", "departamento", 
        "zona", "estado_atencion", "total_beneficiarios", "event_time"
    ]

    for field in required_fields:
        if field not in event:
            return False, f"Missing field: {field}"
        if event[field] in (None, "") and field != "total_beneficiarios":
            return False, f"Empty field: {field}"

    # Validar tipos y rangos (Atrapa inyecciones del productor)
    if not isinstance(event["total_beneficiarios"], int):
        return False, "total_beneficiarios must be an integer"
    if event["total_beneficiarios"] < 0:
        return False, "total_beneficiarios cannot be negative"
        
    if event["zona"] == "ZONA_DESCONOCIDA":
        return False, "Invalid domain value for zona"

    if not str(event["codigo_municipio_dane"]).isdigit():
        return False, "codigo_municipio_dane must be numeric"

    try:
        # Maneja la "Z" de UTC o formatos ISO
        datetime.fromisoformat(event["event_time"].replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return False, "event_time must be a valid ISO timestamp"

    # Verificar duplicados
    cursor.execute("SELECT 1 FROM fact_atencion_streaming WHERE raw_event_id = %s", (event["event_id"],))
    if cursor.fetchone():
        return False, f"Duplicate event_id: {event['event_id']}"

    return True, "Valid event"

def persist_raw_event(event_id, raw_json_str):
    """Guarda el mensaje de Kafka intacto en la base de datos."""
    cursor.execute("""
        INSERT INTO raw_atencion_events (raw_event_id, raw_payload, received_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (raw_event_id) DO NOTHING
    """, (event_id, raw_json_str, datetime.now(timezone.utc).isoformat()))

def persist_valid(event):
    """Guarda en dimensiones y en la tabla de hechos."""
    event_dt = datetime.fromisoformat(event["event_time"].replace('Z', '+00:00'))
    processed_dt = datetime.now(timezone.utc)
    delay = (processed_dt - event_dt).total_seconds()
    date_id = int(event_dt.strftime("%Y%m%d")) 

    # 1. Dimensión Fecha
    cursor.execute("""
        INSERT INTO dim_streaming_date (date_id, year, month, day)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
    """, (date_id, event_dt.year, event_dt.month, event_dt.day))

    # 2. Dimensión Ubicación
    cursor.execute("""
        INSERT INTO dim_streaming_location (codigo_municipio_dane, departamento, zona)
        VALUES (%s, %s, %s)
        ON CONFLICT (codigo_municipio_dane, departamento, zona) DO NOTHING;
    """, (event["codigo_municipio_dane"], event["departamento"], event["zona"]))
    
    cursor.execute("""
        SELECT location_id FROM dim_streaming_location 
        WHERE codigo_municipio_dane = %s AND departamento = %s AND zona = %s
    """, (event["codigo_municipio_dane"], event["departamento"], event["zona"]))
    location_id = cursor.fetchone()[0]

    # 3. Dimensión Estado
    cursor.execute("""
        INSERT INTO dim_streaming_status (estado_atencion)
        VALUES (%s)
        ON CONFLICT (estado_atencion) DO NOTHING;
    """, (event["estado_atencion"],))
    
    cursor.execute("SELECT status_id FROM dim_streaming_status WHERE estado_atencion = %s", (event["estado_atencion"],))
    status_id = cursor.fetchone()[0]

    # 4. Tabla de Hechos
    cursor.execute("""
        INSERT INTO fact_atencion_streaming (
            raw_event_id, location_id, status_id, date_id, 
            total_beneficiarios, event_timestamp, processed_timestamp, processing_delay_seconds
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        event["event_id"], location_id, status_id, date_id,
        event["total_beneficiarios"], event_dt.isoformat(), processed_dt.isoformat(), delay
    ))  

def persist_rejected(event, reason):
    """Guarda eventos que no cumplen el contrato."""
    event_id = event.get("event_id") if isinstance(event, dict) else None
    cursor.execute("""
        INSERT INTO rejected_atencion_events (event_id, raw_event, rejection_reason, rejected_at)
        VALUES (%s, %s, %s, %s)
    """, (
        event_id, json.dumps(event), reason, datetime.now(timezone.utc).isoformat()
    ))

# ============================================================
# BUCLE PRINCIPAL
# ============================================================
def start_consumer():
    init_db()
    print(f"📡 Esperando eventos en el tópico '{TOPIC}'...\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Error de Kafka: {msg.error()}")
                continue

            raw_json_str = msg.value().decode("utf-8")

            try:
                raw_event = json.loads(raw_json_str)
            except json.JSONDecodeError:
                persist_rejected(raw_json_str, "Invalid JSON format")
                continue

            # 1. Validar Contrato
            is_valid, reason = validate_event(raw_event)

            if is_valid:
                # 2. Guardar Crudo
                persist_raw_event(raw_event["event_id"], raw_json_str)
                # 3. Guardar Valido (Hechos/Dim)
                persist_valid(raw_event)
                print(f"✅ Procesado: {raw_event['event_id']} | Mun: {raw_event['codigo_municipio_dane']} | Ben: {raw_event['total_beneficiarios']}")
            else:
                # 4. Guardar Rechazado (Dead Letter Queue)
                persist_rejected(raw_event, reason)
                print(f"❌ Rechazado: {raw_event.get('event_id', 'Unknown')} | Motivo: {reason}")

    except KeyboardInterrupt:
        print("\n🛑 Deteniendo el consumidor...")
    finally:
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    start_consumer()