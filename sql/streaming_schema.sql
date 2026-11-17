-- ============================================================================
-- Esquema de streaming (idempotente). Lo ejecuta streaming/consumer.py al iniciar.
-- Tablas alimentadas por el consumidor + vistas con las métricas del dashboard
-- en tiempo real. Se conecta a la misma base del DW (PostgreSQL, puerto 5433).
-- ============================================================================

-- 1. Eventos crudos (mensaje Kafka intacto)
CREATE TABLE IF NOT EXISTS raw_atencion_events (
    raw_event_id VARCHAR(100) PRIMARY KEY,
    raw_payload  TEXT         NOT NULL,
    kafka_partition INT,
    kafka_offset    BIGINT,
    received_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 2. Dimensiones de streaming
CREATE TABLE IF NOT EXISTS dim_streaming_location (
    location_id SERIAL PRIMARY KEY,
    codigo_municipio_dane VARCHAR(5)   NOT NULL,
    departamento          VARCHAR(100) NOT NULL,
    zona                  VARCHAR(50)  NOT NULL,
    UNIQUE (codigo_municipio_dane, departamento, zona)
);

CREATE TABLE IF NOT EXISTS dim_streaming_status (
    status_id SERIAL PRIMARY KEY,
    estado_atencion VARCHAR(10) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_streaming_date (
    date_id INT PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL
);

-- 3. Hecho de streaming (una fila por evento válido; event_id único = idempotente)
CREATE TABLE IF NOT EXISTS fact_atencion_streaming (
    streaming_id SERIAL PRIMARY KEY,
    raw_event_id VARCHAR(100) NOT NULL UNIQUE REFERENCES raw_atencion_events(raw_event_id),
    location_id  INT NOT NULL REFERENCES dim_streaming_location(location_id),
    status_id    INT NOT NULL REFERENCES dim_streaming_status(status_id),
    date_id      INT NOT NULL REFERENCES dim_streaming_date(date_id),
    total_beneficiarios INT NOT NULL CHECK (total_beneficiarios >= 0),
    event_timestamp     TIMESTAMPTZ,
    processed_timestamp TIMESTAMPTZ,
    processing_delay_seconds DOUBLE PRECISION
);

-- 4. Cola de eventos rechazados (dead letter)
CREATE TABLE IF NOT EXISTS rejected_atencion_events (
    id SERIAL PRIMARY KEY,
    event_id         VARCHAR(100),
    raw_event        TEXT,
    rejection_reason TEXT,
    kafka_partition  INT,
    kafka_offset     BIGINT,
    rejected_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Compatibilidad con la versión anterior del consumidor (tablas ya creadas)
ALTER TABLE raw_atencion_events      ADD COLUMN IF NOT EXISTS kafka_partition INT;
ALTER TABLE raw_atencion_events      ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;
ALTER TABLE rejected_atencion_events ADD COLUMN IF NOT EXISTS kafka_partition INT;
ALTER TABLE rejected_atencion_events ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_stream_event ON fact_atencion_streaming (raw_event_id);

-- ============================================================================
-- MÉTRICAS DE STREAMING (vistas para el dashboard en tiempo real)
-- ============================================================================

-- M1: indicadores globales (tarjetas del dashboard)
CREATE OR REPLACE VIEW vw_stream_kpis AS
SELECT
    (SELECT COUNT(*) FROM fact_atencion_streaming)                         AS eventos_procesados,
    (SELECT COUNT(*) FROM rejected_atencion_events)                        AS eventos_rechazados,
    (SELECT COALESCE(SUM(total_beneficiarios), 0) FROM fact_atencion_streaming) AS beneficiarios_acumulados,
    (SELECT ROUND(AVG(processing_delay_seconds)::numeric, 3) FROM fact_atencion_streaming) AS retardo_promedio_s,
    (SELECT ROUND(MAX(processing_delay_seconds)::numeric, 3) FROM fact_atencion_streaming) AS retardo_maximo_s,
    (SELECT ROUND(100.0 * (SELECT COUNT(*) FROM rejected_atencion_events)
            / NULLIF((SELECT COUNT(*) FROM fact_atencion_streaming)
                   + (SELECT COUNT(*) FROM rejected_atencion_events), 0), 2)) AS pct_rechazo,
    (SELECT MAX(processed_timestamp) FROM fact_atencion_streaming)         AS ultimo_evento_procesado;

-- M2: beneficiarios acumulados por departamento (top / barras)
CREATE OR REPLACE VIEW vw_stream_beneficiarios_departamento AS
SELECT l.departamento,
       COUNT(*)                    AS eventos,
       SUM(f.total_beneficiarios)  AS beneficiarios
FROM fact_atencion_streaming f
JOIN dim_streaming_location l ON l.location_id = f.location_id
GROUP BY l.departamento;

-- M3: rendimiento por minuto (línea de tiempo: eventos y retardo)
CREATE OR REPLACE VIEW vw_stream_throughput_minuto AS
SELECT date_trunc('minute', processed_timestamp)            AS minuto,
       COUNT(*)                                             AS eventos,
       SUM(total_beneficiarios)                             AS beneficiarios,
       ROUND(AVG(processing_delay_seconds)::numeric, 3)     AS retardo_promedio_s
FROM fact_atencion_streaming
GROUP BY 1;

-- M4: distribución por zona y estado de atención
CREATE OR REPLACE VIEW vw_stream_zona_estado AS
SELECT l.zona, s.estado_atencion,
       COUNT(*) AS eventos, SUM(f.total_beneficiarios) AS beneficiarios
FROM fact_atencion_streaming f
JOIN dim_streaming_location l ON l.location_id = f.location_id
JOIN dim_streaming_status   s ON s.status_id   = f.status_id
GROUP BY l.zona, s.estado_atencion;

-- M5: rechazos por motivo
CREATE OR REPLACE VIEW vw_stream_rechazos_motivo AS
SELECT COALESCE(rejection_reason, 'desconocido') AS motivo,
       COUNT(*) AS eventos,
       MAX(rejected_at) AS ultimo_rechazo
FROM rejected_atencion_events
GROUP BY 1;