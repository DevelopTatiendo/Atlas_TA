"""Validador SQL para Atlas Agent.

Garantiza por código que las reglas críticas de negocio se cumplan
antes de que cualquier query llegue a la base de datos:

1. Solo SQL de solo lectura (SELECT / WITH).
2. Mapas de clientes siempre incluyen id_contacto y nunca lat/lon.
3. Consultas de llamadas siempre unen con llamadas_respuestas por id_respuesta.
4. Consultas sobre tablas grandes pasan por COUNT previo (límite 50k filas).
"""

from __future__ import annotations

import re
from typing import Callable

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

_BLOQUEADOS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
    "TRUNCATE", "REPLACE", "GRANT", "REVOKE",
    "LOAD DATA", "INTO OUTFILE", "INTO DUMPFILE",
]

_TABLAS_GRANDES = [
    "FULLCLEAN_CONTACTOS.CONTACTOS",
    "FULLCLEAN_TELEMERCADEO.LLAMADAS",
    "FULLCLEAN_CONTACTOS.CONTACTOS_MOVIMIENTOS",
    "FULLCLEAN_CARTERA.GESTION_COBRO_VISITA",
    "FULLCLEAN_TELEMERCADEO.PEDIDOS",
    "FULLCLEAN_TELEMERCADEO.ENVIO",
    "FULLCLEAN_QUEJAS.QUEJAS",
]

# Condiciones que hacen la consulta suficientemente acotada
_FILTRO_RESTRICTIVO_RE = re.compile(
    r"\bfecha\b|\bbetween\b|\bmes\b"
    r"|\bid_ruta\b|\bid_barrio\b|\bid_categoria\b|\bid_item\b"
    r"|\bbarrio\b|\bruta\b|\bcategoria\b"
    r"|\bhaving\b|\bvalor_total\b|\bsaldo_pendiente\b"
    r"|\bedad_deuda\b|\bvencida\b",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────────────────────

def _norm(sql: str) -> str:
    """Normaliza SQL a mayúsculas con espacios simples."""
    return re.sub(r"\s+", " ", sql.strip()).upper()


# ─────────────────────────────────────────────────────────────────────────────
# Validadores públicos
# ─────────────────────────────────────────────────────────────────────────────

def validar_sql_solo_lectura(sql: str) -> None:
    """Verifica que el SQL sea SELECT o WITH y no contenga escrituras.

    Raises:
        ValueError: si el SQL no es de solo lectura.
    """
    n = _norm(sql)
    if not (n.startswith("SELECT") or n.startswith("WITH")):
        raise ValueError("Solo se permiten consultas SELECT o WITH.")
    for palabra in _BLOQUEADOS:
        if re.search(rf"\b{re.escape(palabra)}\b", n):
            raise ValueError(f"SQL bloqueado — contiene '{palabra}'.")


def validar_sql_mapa_clientes(sql: str) -> None:
    """Valida SQL destinado a mapas de clientes.

    Reglas aplicadas:
    - Debe ser de solo lectura.
    - Debe devolver id_contacto (c.id AS id_contacto o similar).
    - No debe devolver lat, lon, latitud ni longitud (vienen del cache).

    Raises:
        ValueError: si alguna regla se viola.
    """
    n = _norm(sql)
    validar_sql_solo_lectura(sql)

    if "ID_CONTACTO" not in n:
        raise ValueError(
            "El SQL para mapas debe devolver id_contacto. "
            "Agrega 'c.id AS id_contacto' (u otro alias) en el SELECT."
        )

    if re.search(r"\bLAT\b|\bLON\b|\bLATITUD\b|\bLONGITUD\b", n):
        raise ValueError(
            "El SQL de clientes no debe traer lat/lon. "
            "Las coordenadas se anexan internamente desde el cache de coordenadas."
        )


def validar_join_llamadas(sql: str) -> None:
    """Verifica que consultas con la tabla llamadas unan llamadas_respuestas.

    Una consulta que usa `llamadas` sin `llamadas_respuestas` no puede
    leer contestada, es_venta ni contactabilidad real de forma confiable.

    Raises:
        ValueError: si se usa llamadas sin el join requerido.
    """
    n = _norm(sql)

    # Detectar uso de la tabla llamadas pero NO de llamadas_respuestas
    # (?!_) evita que coincida con el prefijo de LLAMADAS_RESPUESTAS
    usa_llamadas = bool(re.search(r"\bLLAMADAS\b(?!_)", n))

    if not usa_llamadas:
        return

    if "LLAMADAS_RESPUESTAS" not in n:
        raise ValueError(
            "Toda consulta de llamadas debe incluir JOIN con "
            "fullclean_telemercadeo.llamadas_respuestas lr ON lr.Id = l.id_respuesta. "
            "Sin este join no es posible leer contestada, es_venta ni contactabilidad real."
        )

    if "ID_RESPUESTA" not in n:
        raise ValueError(
            "La consulta de llamadas debe unirse por l.id_respuesta "
            "(ON lr.Id = l.id_respuesta)."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Protocolo COUNT previo
# ─────────────────────────────────────────────────────────────────────────────

def requiere_count_previo(sql: str) -> bool:
    """Determina si la consulta debe ejecutarse con COUNT previo.

    Aplica cuando usa una tabla grande, sin LIMIT y sin filtros restrictivos.
    """
    n = _norm(sql)
    usa_tabla_grande = any(t in n for t in _TABLAS_GRANDES)
    if not usa_tabla_grande:
        return False
    if re.search(r"\bLIMIT\b", n):
        return False
    if _FILTRO_RESTRICTIVO_RE.search(sql):
        return False
    return True


def construir_count_sql(sql: str) -> str:
    """Envuelve el SQL en un SELECT COUNT(*) para estimar volumen."""
    return f"SELECT COUNT(*) AS total FROM ({sql.strip().rstrip(';')}) AS _conteo_atlas"


def validar_volumen(
    sql: str,
    schema_sql: str,
    sql_read_fn: Callable,
    max_rows: int = 50_000,
) -> dict:
    """Ejecuta COUNT previo si aplica y bloquea consultas demasiado grandes.

    Args:
        sql:         Query original a evaluar.
        schema_sql:  Schema por defecto para ejecutar el COUNT.
        sql_read_fn: Función sql_read(sql, schema) para ejecutar queries.
        max_rows:    Límite de filas permitido (default 50,000).

    Returns:
        {"ok": True} si pasa, o {"ok": False, "error": ..., "count": ...} si excede.
    """
    if not requiere_count_previo(sql):
        return {"ok": True}

    try:
        df = sql_read_fn(construir_count_sql(sql), schema=schema_sql)
        total = int(df.iloc[0, 0]) if not df.empty else 0
    except Exception as e:
        # Si el COUNT falla no bloqueamos — dejamos pasar con aviso
        return {"ok": True, "aviso": f"COUNT previo no pudo ejecutarse: {e}"}

    if total > max_rows:
        return {
            "ok": False,
            "error": (
                f"La consulta retornaría {total:,} registros (límite: {max_rows:,}). "
                "Acota por fecha, barrio, categoría, ruta, producto o monto."
            ),
            "count": total,
        }

    return {"ok": True, "count": total}
