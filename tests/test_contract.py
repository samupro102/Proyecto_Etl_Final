import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "streaming"))
import contract   # noqa: E402

BASE = {
    "event_id": "r20261115T000000-1", "event_type": "atencion_reportada", "schema_version": 1,
    "event_time": "2026-11-15T12:00:00+00:00", "simulated": True, "fact_key": 1,
    "codigo_municipio_dane": "05001", "departamento": "ANTIOQUIA", "zona": "CABECERA",
    "estado_atencion": "A", "total_beneficiarios": 10,
}


def con(**cambios):
    e = dict(BASE)
    e.update(cambios)
    return e


def test_evento_valido():
    assert contract.validar(BASE) == (True, "ok")


def test_corrupciones_del_productor():
    # mismas 4 violaciones que producer.corromper()
    sin_total = dict(BASE); del sin_total["total_beneficiarios"]
    for malo in (sin_total, con(codigo_municipio_dane="ABC"),
                 con(total_beneficiarios=-5), con(zona="ZONA_DESCONOCIDA")):
        assert contract.validar(malo)[0] is False


def test_tipos_y_dominios():
    assert not contract.validar(con(total_beneficiarios=True))[0]       # bool no es entero
    assert not contract.validar(con(total_beneficiarios="10"))[0]
    assert not contract.validar(con(total_beneficiarios=None))[0]
    assert not contract.validar(con(estado_atencion="X"))[0]
    assert not contract.validar(con(codigo_municipio_dane="5001"))[0]   # sin relleno a 5
    assert not contract.validar(con(event_time="ayer"))[0]
    assert not contract.validar(con(event_time="2026-11-15T12:00:00"))[0]  # sin zona horaria
    assert not contract.validar(con(schema_version=2))[0]
    assert not contract.validar(con(event_type="otro"))[0]
    assert contract.validar(con(event_time="2026-11-15T12:00:00Z"))[0]


def test_no_es_objeto():
    assert contract.validar([1, 2])[0] is False
    assert contract.validar("texto")[0] is False