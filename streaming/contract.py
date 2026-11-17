
import re
from datetime import datetime

TOPIC = "icbf.atencion.reportada"
EVENT_TYPE = "atencion_reportada"
SCHEMA_VERSION = 1
EVENT_KEY = "codigo_municipio_dane"
EVENT_ID_FIELD = "event_id"
TIMESTAMP_FIELD = "event_time"

ZONAS = {"CABECERA", "RESTO", "SIN INFORMACIÓN"}
ESTADOS = {"A", "I"}
_DANE = re.compile(r"^\d{5}$")

# campo -> (tipo esperado, descripción)
CONTRATO = {
    "event_id": (str, "Identificador único del evento: <replay_id>-<fact_key>"),
    "event_type": (str, f"Constante '{EVENT_TYPE}'"),
    "schema_version": (int, f"Versión del contrato (= {SCHEMA_VERSION})"),
    "event_time": (str, "Instante de publicación, ISO 8601 con zona horaria"),
    "simulated": (bool, "true: el evento es un replay de datos históricos"),
    "fact_key": (int, "fact_key de fact_atencion en el DW"),
    "codigo_municipio_dane": (str, "Código DANE de 5 dígitos (llave del mensaje)"),
    "departamento": (str, "Departamento, no vacío"),
    "zona": (str, "Uno de: " + ", ".join(sorted(ZONAS))),
    "estado_atencion": (str, "A (activa) o I (inactiva)"),
    "total_beneficiarios": (int, "Entero >= 0"),
}


def parse_event_time(valor):
    """Convierte event_time a datetime con zona horaria; lanza ValueError si no es válido."""
    if not isinstance(valor, str):
        raise ValueError("event_time debe ser texto ISO 8601")
    dt = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("event_time debe incluir zona horaria")
    return dt


def validar(evento):
    """Devuelve (True, 'ok') o (False, motivo). Nunca lanza excepciones."""
    if not isinstance(evento, dict):
        return False, "El mensaje no es un objeto JSON"

    for campo, (tipo, _) in CONTRATO.items():
        if campo not in evento:
            return False, f"Campo obligatorio ausente: {campo}"
        valor = evento[campo]
        if valor is None:
            return False, f"Campo nulo: {campo}"
        # bool es subclase de int: se rechaza donde se espera un entero
        if tipo is int and (isinstance(valor, bool) or not isinstance(valor, int)):
            return False, f"Tipo inválido en {campo}: se esperaba entero"
        if tipo is not int and not isinstance(valor, tipo):
            return False, f"Tipo inválido en {campo}: se esperaba {tipo.__name__}"

    if evento["event_type"] != EVENT_TYPE:
        return False, f"event_type inesperado: {evento['event_type']}"
    if evento["schema_version"] != SCHEMA_VERSION:
        return False, f"schema_version no soportada: {evento['schema_version']}"
    if not evento["event_id"].strip():
        return False, "event_id vacío"
    if not _DANE.match(evento["codigo_municipio_dane"]):
        return False, "codigo_municipio_dane debe tener 5 dígitos"
    if not evento["departamento"].strip():
        return False, "departamento vacío"
    if evento["zona"] not in ZONAS:
        return False, f"zona fuera de dominio: {evento['zona']}"
    if evento["estado_atencion"] not in ESTADOS:
        return False, f"estado_atencion fuera de dominio: {evento['estado_atencion']}"
    if evento["total_beneficiarios"] < 0:
        return False, "total_beneficiarios no puede ser negativo"
    try:
        parse_event_time(evento["event_time"])
    except ValueError as e:
        return False, str(e) if "event_time" in str(e) else "event_time no es ISO 8601 válido"
    return True, "ok"