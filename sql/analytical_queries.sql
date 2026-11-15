-- PostgreSQL: el DW vive en la base configurada en .env (esquema public)

-- ============================================================
-- R1: Distribución territorial de beneficiarios entre departamentos
-- ============================================================
SELECT
    g.departamento_uds AS departamento,
    SUM(f.total_beneficiarios) AS total_beneficiarios,
    ROUND(
        100.0 * SUM(f.total_beneficiarios) / (SELECT SUM(total_beneficiarios) FROM fact_atencion),
        2
    ) AS porcentaje_participacion
FROM fact_atencion f
JOIN dim_geography g ON f.geography_key = g.geography_key
GROUP BY g.departamento_uds
ORDER BY total_beneficiarios DESC;


-- ============================================================
-- R2: Top 5 municipios por beneficiarios, dentro de cada departamento
-- ============================================================
WITH municipios_por_departamento AS (
    SELECT
        g.departamento_uds AS departamento,
        g.municipio_uds AS municipio,
        SUM(f.total_beneficiarios) AS total_beneficiarios,
        ROW_NUMBER() OVER (
            PARTITION BY g.departamento_uds
            ORDER BY SUM(f.total_beneficiarios) DESC
        ) AS ranking
    FROM fact_atencion f
    JOIN dim_geography g ON f.geography_key = g.geography_key
    GROUP BY g.departamento_uds, g.municipio_uds
)
SELECT departamento, municipio, total_beneficiarios, ranking
FROM municipios_por_departamento
WHERE ranking <= 5
ORDER BY departamento, ranking;


-- ============================================================
-- R3.1: Ranking nacional de tipos de servicio
-- ============================================================
SELECT
    s.tipo_servicio,
    SUM(f.total_beneficiarios) AS total_beneficiarios,
    ROUND(
        100.0 * SUM(f.total_beneficiarios) / (SELECT SUM(total_beneficiarios) FROM fact_atencion),
        2
    ) AS porcentaje_participacion
FROM fact_atencion f
JOIN dim_service s ON f.service_key = s.service_key
GROUP BY s.tipo_servicio
ORDER BY total_beneficiarios DESC;

-- ============================================================
-- R3.2: Cruce tipo de servicio x departamento
-- ============================================================
SELECT
    g.departamento_uds AS departamento,
    s.tipo_servicio,
    SUM(f.total_beneficiarios) AS total_beneficiarios
FROM fact_atencion f
JOIN dim_geography g ON f.geography_key = g.geography_key
JOIN dim_service s ON f.service_key = s.service_key
GROUP BY g.departamento_uds, s.tipo_servicio
ORDER BY departamento, total_beneficiarios DESC;


-- ============================================================
-- R4.1: Por sexo
-- ============================================================
SELECT
    bp.sexo,
    SUM(f.total_beneficiarios) AS total_beneficiarios,
    ROUND(100.0 * SUM(f.total_beneficiarios) / (SELECT SUM(total_beneficiarios) FROM fact_atencion), 2) AS porcentaje
FROM fact_atencion f
JOIN dim_beneficiary_profile bp ON f.beneficiary_profile_key = bp.beneficiary_profile_key
GROUP BY bp.sexo
ORDER BY total_beneficiarios DESC;

-- ============================================================
-- R4.2: Por rango de edad
-- ============================================================
SELECT
    bp.rango_edad,
    SUM(f.total_beneficiarios) AS total_beneficiarios,
    ROUND(100.0 * SUM(f.total_beneficiarios) / (SELECT SUM(total_beneficiarios) FROM fact_atencion), 2) AS porcentaje
FROM fact_atencion f
JOIN dim_beneficiary_profile bp ON f.beneficiary_profile_key = bp.beneficiary_profile_key
GROUP BY bp.rango_edad
ORDER BY total_beneficiarios DESC;

-- ============================================================
-- R4.3: Por grupo étnico
-- ============================================================
SELECT
    bp.grupo_etnico,
    SUM(f.total_beneficiarios) AS total_beneficiarios,
    ROUND(100.0 * SUM(f.total_beneficiarios) / (SELECT SUM(total_beneficiarios) FROM fact_atencion), 2) AS porcentaje
FROM fact_atencion f
JOIN dim_beneficiary_profile bp ON f.beneficiary_profile_key = bp.beneficiary_profile_key
GROUP BY bp.grupo_etnico
ORDER BY total_beneficiarios DESC;

-- ============================================================
-- R4.4: Por discapacidad
-- ============================================================
SELECT
    bp.presenta_discapacidad,
    SUM(f.total_beneficiarios) AS total_beneficiarios,
    ROUND(100.0 * SUM(f.total_beneficiarios) / (SELECT SUM(total_beneficiarios) FROM fact_atencion), 2) AS porcentaje
FROM fact_atencion f
JOIN dim_beneficiary_profile bp ON f.beneficiary_profile_key = bp.beneficiary_profile_key
GROUP BY bp.presenta_discapacidad
ORDER BY total_beneficiarios DESC;


-- ============================================================
-- R5: Índice de concentración de vulnerabilidad por departamento
-- ============================================================
SELECT
    g.departamento_uds AS departamento,
    SUM(CASE WHEN bp.grupo_etnico = 'INDÍGENA' THEN f.total_beneficiarios ELSE 0 END) AS beneficiarios_indigenas,
    SUM(CASE WHEN bp.presenta_discapacidad = 'SI' THEN f.total_beneficiarios ELSE 0 END) AS beneficiarios_con_discapacidad,
    SUM(CASE WHEN z.nombre_zona = 'RESTO' THEN f.total_beneficiarios ELSE 0 END) AS beneficiarios_zona_rural,
    SUM(f.total_beneficiarios) AS total_beneficiarios,
    ROUND(
        100.0 * SUM(CASE WHEN bp.grupo_etnico = 'INDÍGENA' THEN f.total_beneficiarios ELSE 0 END)
        / SUM(f.total_beneficiarios), 2
    ) AS porcentaje_indigena
FROM fact_atencion f
JOIN dim_geography g ON f.geography_key = g.geography_key
JOIN dim_beneficiary_profile bp ON f.beneficiary_profile_key = bp.beneficiary_profile_key
JOIN dim_zone z ON f.zone_key = z.zone_key
GROUP BY g.departamento_uds
ORDER BY porcentaje_indigena DESC;