"""Trainer dominio: pedidos.

Cubre: pedido válido (5 filtros), valor de pedidos, frecuencia,
primera compra, reactivación, ruta + valor.
"""

from __future__ import annotations
from agente.vanna_sql.trainer import DOC_BLOCKS, SQL_EXAMPLES

DOMINIO = "pedidos"

DOCS = [DOC_BLOCKS[i] for i in [5, 11]]
# DOC[5]  = pedido válido — los 5 filtros obligatorios
# DOC[11] = cadena producto en pedidos (pedidos_det → items → lineas/productos)

EXAMPLES = [SQL_EXAMPLES[i] for i in [1, 2, 10, 11, 12, 13, 19]]
# SQL[1]  = clientes más activos por cantidad de pedidos válidos
# SQL[2]  = clientes frecuentes (múltiples pedidos Q1 2026)
# SQL[10] = pre-churn (compraron 2025, no en 2026)
# SQL[11] = nuevos — primera compra en período
# SQL[12] = reactivados (>180 días sin comprar)
# SQL[13] = clientes con factura generada
# SQL[19] = clientes ruta 2 Cali con pedidos > 200k en abril 2026


def train(vn, verbose: bool = True) -> None:
    """Entrena reglas y ejemplos del dominio pedidos."""
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
