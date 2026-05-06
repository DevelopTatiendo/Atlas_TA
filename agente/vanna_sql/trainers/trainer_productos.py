"""Trainer dominio: productos, líneas y presentaciones.

Cubre: items → productos/presentaciones/lineas, filtros por línea
(BLUE PET, FULLIMP, SAVITRI, ZAGUS), catálogo de productos.
"""

from __future__ import annotations
from agente.vanna_sql.trainer import DOC_BLOCKS, SQL_EXAMPLES

DOMINIO = "productos"

DOCS = [DOC_BLOCKS[i] for i in [13]]
# DOC[13] = catálogo de productos principales y presentaciones con IDs reales

EXAMPLES = [SQL_EXAMPLES[i] for i in [4, 5]]
# SQL[4] = clientes que compraron BLUE PET (CO Bogotá, 2026)
# SQL[5] = clientes que compraron producto 2 en presentación 103 (feb 2026)


def train(vn, verbose: bool = True) -> None:
    """Entrena reglas y ejemplos del dominio productos."""
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
