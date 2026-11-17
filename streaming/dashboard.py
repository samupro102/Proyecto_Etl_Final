
import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

st.set_page_config(page_title="ICBF — Streaming en tiempo real", layout="wide")
REFRESCO = st.sidebar.slider("Actualizar cada (s)", 1, 30, 3)


@st.cache_resource
def engine():
    return create_engine(URL.create(
        "postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER", "postgres"),
        password=(os.getenv("POSTGRES_PASSWORD") or "").strip("\"'"),
        host=os.getenv("STREAM_PG_HOST", "localhost"),
        port=int(os.getenv("STREAM_PG_PORT", "5433")),
        database=os.getenv("DW_POSTGRES_DB") or os.getenv("POSTGRES_DB", "postgres"),
    ))


def q(sql):
    return pd.read_sql(sql, engine())


st.title("Atención ICBF — métricas de streaming")
st.caption("Fuente: tópico Kafka icbf.atencion.reportada → consumer.py → PostgreSQL (vistas vw_stream_*). "
           "Los eventos son un replay simulado de fact_atencion.")

try:
    k = q("SELECT * FROM vw_stream_kpis").iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Eventos procesados", f"{int(k.eventos_procesados):,}")
    c2.metric("Beneficiarios acumulados", f"{int(k.beneficiarios_acumulados):,}")
    c3.metric("Retardo promedio (s)", k.retardo_promedio_s if pd.notna(k.retardo_promedio_s) else "—")
    c4.metric("Rechazados", f"{int(k.eventos_rechazados):,}",
              f"{k.pct_rechazo}%" if pd.notna(k.pct_rechazo) else None, delta_color="inverse")

    a, b = st.columns(2)
    dep = q("SELECT departamento, beneficiarios FROM vw_stream_beneficiarios_departamento "
            "ORDER BY beneficiarios DESC LIMIT 10")
    a.subheader("Beneficiarios por departamento (top 10)")
    a.bar_chart(dep.set_index("departamento"))
    t = q("SELECT minuto, eventos FROM vw_stream_throughput_minuto ORDER BY minuto")
    b.subheader("Eventos por minuto")
    b.line_chart(t.set_index("minuto"))

    st.subheader("Rechazos por motivo")
    st.dataframe(q("SELECT * FROM vw_stream_rechazos_motivo ORDER BY eventos DESC"), hide_index=True)
except Exception as e:
    st.warning(f"Aún no hay datos o la base no está lista: {e}")

time.sleep(REFRESCO)
st.rerun()