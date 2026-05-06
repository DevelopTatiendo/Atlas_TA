"""Trainer dominio: rutas de cobro.

Cubre: cadena barrio → rutas_cobro_zonas → rutas_cobro,
filtros por ruta específica, cobertura territorial.
"""

from __future__ import annotations
from agente.vanna_sql.trainer import DOC_BLOCKS, SQL_EXAMPLES

DOMINIO = "rutas"

DOCS = [DOC_BLOCKS[i] for i in [9]]
# DOC[9] = cadena de rutas de cobro (contactos → barrios → rcz → rutas_cobro)

EXAMPLES = [SQL_EXAMPLES[i] for i in [15, 16]]
# SQL[15] = clientes activos de la ruta de cobro 13 (CO Medellín)
# SQL[16] = clientes gestionados por vendedor específico


def train(vn, verbose: bool = True) -> None:
    """Entrena reglas y ejemplos del dominio rutas de cobro."""
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
