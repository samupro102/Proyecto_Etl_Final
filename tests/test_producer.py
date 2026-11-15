import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "streaming"))
import contract  # noqa: E402
import producer  # noqa: E402


def _df():
    return pd.DataFrame({
        "fact_key": [1, 2], "codigo_municipio_dane": ["05001", "44001"],
        "departamento": ["ANTIOQUIA", "LA GUAJIRA"], "zona": ["CABECERA", "RESTO"],
        "estado_atencion": ["A", "I"], "total_beneficiarios": [3, 7],
    })


def _con_hora(e):
    e = dict(e); e["event_time"] = "2026-11-15T12:00:00+00:00"; return e


def test_eventos_del_productor_cumplen_el_contrato():
    for e in producer.construir_eventos(_df(), "rTEST"):
        assert contract.validar(_con_hora(e)) == (True, "ok")


def test_eventos_corrompidos_son_rechazados():
    base = _con_hora(producer.construir_eventos(_df(), "rTEST")[0])
    for tipo in range(4):
        assert contract.validar(producer.corromper(base, tipo))[0] is False