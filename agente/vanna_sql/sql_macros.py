"""Macros SQL reutilizables para Atlas TA.

Fuente única de verdad para los fragmentos SQL críticos de negocio.
Los ejemplos del trainer y cualquier código que construya SQL deben
referenciar estas constantes en lugar de duplicarlos manualmente.

Uso en el trainer:
    from agente.vanna_sql.sql_macros import MACRO_PEDIDO_VALIDO
    doc = f"Filtros obligatorios de pedido válido:\n{MACRO_PEDIDO_VALIDO}"

Uso en SQL directo (f-string):
    sql = f\"\"\"
    SELECT c.id AS id_contacto, c.nombre
    FROM fullclean_contactos.contactos c
    {MACRO_JOIN_CO}
    {MACRO_JOIN_LLAMADAS_RESPUESTAS}
    WHERE ci.id_centroope = 3
      AND {MACRO_LLAMADA_CONTESTADA}
    \"\"\"
"""

from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# Pedidos
# ─────────────────────────────────────────────────────────────────────────────

MACRO_PEDIDO_VALIDO = """\
pe.estado_pedido = 1
  AND pe.anulada = 0
  AND pe.autorizar IN (1, 2)
  AND pe.autorizacion_descuento = 0
  AND pe.tipo_documento < 2"""

# ─────────────────────────────────────────────────────────────────────────────
# Llamadas y contactabilidad
# ─────────────────────────────────────────────────────────────────────────────

MACRO_JOIN_LLAMADAS_RESPUESTAS = """\
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.Id = l.id_respuesta"""

MACRO_LLAMADA_CONTESTADA = """\
l.estado = 1
  AND lr.contestada = 1"""

MACRO_LLAMADA_LIMPIA = """\
l.estado = 1
  AND l.id_contacto <> 0
  AND l.id_vendedor NOT IN (0, 1)"""

# ─────────────────────────────────────────────────────────────────────────────
# Centro de Operación
# ─────────────────────────────────────────────────────────────────────────────

MACRO_JOIN_CO = """\
INNER JOIN fullclean_contactos.ciudades ci
    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce
    ON ce.id = ci.id_centroope"""

# ─────────────────────────────────────────────────────────────────────────────
# Clientes
# ─────────────────────────────────────────────────────────────────────────────

MACRO_CLIENTE_ACTIVO = """\
(COALESCE(c.cant_obsequios, 0) > 0 OR c.estado_cxc IN (0, 1))"""

MACRO_CANAL_DIRECTO = """\
c.id_canal = 2"""

# ─────────────────────────────────────────────────────────────────────────────
# Rutas de cobro
# ─────────────────────────────────────────────────────────────────────────────

MACRO_JOIN_RUTA_COBRO = """\
JOIN fullclean_contactos.barrios b
    ON b.Id = c.id_barrio
JOIN fullclean_contactos.rutas_cobro_zonas rcz
    ON rcz.id_barrio = b.Id
JOIN fullclean_contactos.rutas_cobro rc
    ON rc.id = rcz.id_ruta_cobro"""

# ─────────────────────────────────────────────────────────────────────────────
# Utilidades de composición
# ─────────────────────────────────────────────────────────────────────────────

def pedido_valido_donde(alias: str = "pe") -> str:
    """Devuelve los 5 filtros de pedido válido con el alias dado."""
    return (
        f"{alias}.estado_pedido = 1\n"
        f"  AND {alias}.anulada = 0\n"
        f"  AND {alias}.autorizar IN (1, 2)\n"
        f"  AND {alias}.autorizacion_descuento = 0\n"
        f"  AND {alias}.tipo_documento < 2"
    )


def join_llamadas_respuestas(alias_l: str = "l", alias_lr: str = "lr") -> str:
    """Devuelve el INNER JOIN de llamadas_respuestas con los aliases dados."""
    return (
        f"INNER JOIN fullclean_telemercadeo.llamadas_respuestas {alias_lr}\n"
        f"    ON {alias_lr}.Id = {alias_l}.id_respuesta"
    )
