
import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
import contract  # noqa: E402

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_SQL = BASE_DIR / "sql" / "streaming_schema.sql"
load_dotenv(BASE_DIR / ".env")

_parar = False


def _senal(*_):
    global _parar
    _parar = True


def conectar_pg():
    conn = psycopg2.connect(
        dbname=os.getenv("DW_POSTGRES_DB") or os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=(os.getenv("POSTGRES_PASSWORD") or "").strip("\"'"),
        host=os.getenv("STREAM_PG_HOST", "localhost"),
        port=os.getenv("STREAM_PG_PORT", "5433"),
    )
    conn.autocommit = False  # una transacción por evento
    return conn


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    conn.commit()
    print("✅ Esquema de streaming y vistas de métricas listos.")


def persistir_valido(cur, evento, raw, particion, offset):
    event_dt = contract.parse_event_time(evento["event_time"])
    procesado = datetime.now(timezone.utc)
    retardo = (procesado - event_dt).total_seconds()
    date_id = int(event_dt.strftime("%Y%m%d"))

    cur.execute(
        """INSERT INTO raw_atencion_events (raw_event_id, raw_payload, kafka_partition, kafka_offset)
           VALUES (%s, %s, %s, %s) ON CONFLICT (raw_event_id) DO NOTHING""",
        (evento["event_id"], raw, particion, offset),
    )
    cur.execute(
        """INSERT INTO dim_streaming_date (date_id, year, month, day)
           VALUES (%s, %s, %s, %s) ON CONFLICT (date_id) DO NOTHING""",
        (date_id, event_dt.year, event_dt.month, event_dt.day),
    )
    cur.execute(
        """INSERT INTO dim_streaming_location (codigo_municipio_dane, departamento, zona)
           VALUES (%s, %s, %s)
           ON CONFLICT (codigo_municipio_dane, departamento, zona) DO NOTHING""",
        (evento["codigo_municipio_dane"], evento["departamento"], evento["zona"]),
    )
    cur.execute(
        """SELECT location_id FROM dim_streaming_location
           WHERE codigo_municipio_dane=%s AND departamento=%s AND zona=%s""",
        (evento["codigo_municipio_dane"], evento["departamento"], evento["zona"]),
    )
    location_id = cur.fetchone()[0]
    cur.execute(
        """INSERT INTO dim_streaming_status (estado_atencion) VALUES (%s)
           ON CONFLICT (estado_atencion) DO NOTHING""",
        (evento["estado_atencion"],),
    )
    cur.execute("SELECT status_id FROM dim_streaming_status WHERE estado_atencion=%s",
                (evento["estado_atencion"],))
    status_id = cur.fetchone()[0]

    cur.execute(
        """INSERT INTO fact_atencion_streaming
               (raw_event_id, location_id, status_id, date_id, total_beneficiarios,
                event_timestamp, processed_timestamp, processing_delay_seconds)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (raw_event_id) DO NOTHING""",
        (evento["event_id"], location_id, status_id, date_id,
         evento["total_beneficiarios"], event_dt, procesado, retardo),
    )
    return cur.rowcount == 1   # False -> duplicado ya procesado


def persistir_rechazado(cur, evento, raw, motivo, particion, offset):
    event_id = evento.get("event_id") if isinstance(evento, dict) else None
    cur.execute(
        """INSERT INTO rejected_atencion_events
               (event_id, raw_event, rejection_reason, kafka_partition, kafka_offset)
           VALUES (%s,%s,%s,%s,%s)""",
        (str(event_id) if event_id is not None else None, raw, motivo, particion, offset),
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--group", default="icbf-streaming-team", help="group.id de Kafka")
    ap.add_argument("--from-beginning", action="store_true",
                    help="si el grupo no tiene offset, empezar por el inicio (earliest); si no, latest")
    ap.add_argument("--max-events", type=int, default=0, help="terminar tras N mensajes (0 = sin límite)")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _senal)
    signal.signal(signal.SIGTERM, _senal)

    try:
        conn = conectar_pg()
    except Exception as e:
        sys.exit(f"❌ No se pudo conectar a PostgreSQL: {e}")
    init_db(conn)

    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"),
        "group.id": args.group,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": False,      # commit manual tras persistir
    })
    consumer.subscribe([contract.TOPIC])
    print(f"📡 Grupo '{args.group}' escuchando '{contract.TOPIC}' (Ctrl+C para detener)\n")

    ok = rechazados = duplicados = 0
    t0 = time.time()
    try:
        while not _parar:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"⚠️  Error de Kafka: {msg.error()}")
                continue

            raw = msg.value().decode("utf-8", errors="replace")
            particion, offset = msg.partition(), msg.offset()
            try:
                evento = json.loads(raw)
            except json.JSONDecodeError:
                evento = None
                valido, motivo = False, "JSON inválido"
            else:
                valido, motivo = contract.validar(evento)

            try:
                with conn.cursor() as cur:
                    if valido:
                        nuevo = persistir_valido(cur, evento, raw, particion, offset)
                    else:
                        persistir_rechazado(cur, evento, raw, motivo, particion, offset)
                conn.commit()
            except Exception as e:      # falla de BD: no se confirma offset; se reintentará al reiniciar
                conn.rollback()
                print(f"❌ Error de base de datos, se detiene sin confirmar el offset: {e}")
                break

            consumer.commit(message=msg, asynchronous=False)

            if valido and nuevo:
                ok += 1
                print(f"✅ {evento['event_id']} | mun={evento['codigo_municipio_dane']} "
                      f"| ben={evento['total_beneficiarios']} | p{particion}@{offset}")
            elif valido:
                duplicados += 1
                print(f"♻️  Duplicado ignorado: {evento['event_id']}")
            else:
                rechazados += 1
                eid = evento.get("event_id") if isinstance(evento, dict) else "?"
                print(f"🚫 Rechazado {eid} | motivo: {motivo}")

            if args.max_events and (ok + rechazados + duplicados) >= args.max_events:
                break
    finally:
        consumer.close()
        conn.close()
        seg = time.time() - t0
        print(f"\n🛑 Consumidor detenido. procesados={ok} rechazados={rechazados} "
              f"duplicados={duplicados} en {seg:.1f}s")


if __name__ == "__main__":
    main()