"""Trainer dominio: puntos y movimientos de clientes.

Cubre: contactos_movimientos, conceptos, acumulación, redención,
puntos netos por período.
"""

from __future__ import annotations
from agente.vanna_sql.trainer import DOC_BLOCKS, SQL_EXAMPLES

DOMINIO = "puntos"

DOCS = [DOC_BLOCKS[i] for i in [10]]
# DOC[10] = puntos y movimientos (contactos_movimientos + conceptos, valor +/-)

EXAMPLES = [SQL_EXAMPLES[i] for i in [14]]
# SQL[14] = clientes con movimientos de puntos en Q1 2026 (CO Medellín)


def train(vn, verbose: bool = True) -> None:
    """Entrena reglas y ejemplos del dominio puntos."""
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
