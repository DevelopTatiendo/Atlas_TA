"""Trainer dominio: mapas de clientes.

Cubre: reglas de coordenadas (nunca lat/lon en SQL de clientes),
cache de coordenadas, vwEventos, campo_valor para visualización.
"""

from __future__ import annotations
from agente.vanna_sql.trainer import DOC_BLOCKS, SQL_EXAMPLES

DOMINIO = "mapas"

DOCS = [DOC_BLOCKS[i] for i in [14]]
# DOC[14] = coordenadas de clientes — cache Parquet, nunca lat/lon en SQL de clientes

# Regla extra específica de mapas (no está en el trainer principal como bloque separado)
_DOC_EXTRA = """
REGLA CRÍTICA — SQL para mapas de clientes:
El SQL que se pasa a generar_mapa_clientes DEBE:
  1. Devolver c.id AS id_contacto (obligatorio para hacer merge con cache de coords).
  2. Devolver atributos útiles de negocio: nombre, barrio, ruta_cobro, valor_pedidos, etc.
  3. NUNCA devolver lat, lon, latitud ni longitud — las coordenadas vienen del cache.
  4. Incluir filtro de CO (ci.id_centroope = [N]).
  5. Aplicar filtros de negocio obligatorios por dominio (pedido válido, llamadas, etc.).

Contrato de columnas del SQL de mapa:
  CORRECTO:  id_contacto, nombre, barrio, ruta_cobro, valor_pedidos
  INCORRECTO: id_contacto, nombre, lat, lon
"""

DOCS = [DOC_BLOCKS[14], _DOC_EXTRA]

EXAMPLES = [SQL_EXAMPLES[i] for i in [19, 20]]
# SQL[19] = ruta Cali + pedidos > 200k → mapa por valor_pedidos
# SQL[20] = ranking score pedidos+llamadas → mapa por score_comercial


def train(vn, verbose: bool = True) -> None:
    """Entrena reglas y ejemplos del dominio mapas."""
    def log(msg):
        if verbose: print(msg)

    log(f"\n=== [{DOMINIO}] Docs — {len(DOCS)} bloques ===")
    for i, doc in enumerate(DOCS, 1):
        vn.train(documentation=doc)
        log(f"  [DOC {i}] OK")

    log(f"\n=== [{DOMINIO}] SQL — {len(EXAMPLES)} ejemplos ===")
    for i, ex in enumerate(EXAMPLES, 1):
        vn.train(question=ex["question"], sql=ex["sql"])
        log(f"  [SQL {i}] {ex['question'][:65]}...")

    log(f"\n✔ [{DOMINIO}] completo — Docs:{len(DOCS)} SQL:{len(EXAMPLES)}")
