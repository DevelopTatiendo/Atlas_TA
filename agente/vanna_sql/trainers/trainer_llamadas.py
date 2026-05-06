"""Trainer dominio: llamadas y contactabilidad.

Cubre: INNER JOIN obligatorio con llamadas_respuestas, contestada,
es_venta, métricas de contactabilidad y conversión.

Regla crítica: toda consulta de llamadas DEBE unir llamadas_respuestas
por l.id_respuesta. Sin ese join no existe lectura confiable de
contestada ni es_venta.
"""

from __future__ import annotations
from agente.vanna_sql.trainer import DOC_BLOCKS, SQL_EXAMPLES

DOMINIO = "llamadas"

DOCS = [DOC_BLOCKS[i] for i in [6, 7]]
# DOC[6] = contactabilidad real (INNER JOIN, contestada=1, es_venta=1)
# DOC[7] = regla crítica — llamadas siempre con respuesta (INNER JOIN obligatorio)

EXAMPLES = [SQL_EXAMPLES[i] for i in [3, 8, 9, 17, 18]]
# SQL[3]  = contestan pero no compran (alto potencial)
# SQL[8]  = clientes con > 3 llamadas contestadas
# SQL[9]  = clientes sin contacto real en últimos 60 días
# SQL[17] = muchas llamadas contestadas pero ningún pedido en 2026
# SQL[18] = clientes que contestaron llamada Y tienen pedido mismo mes


def train(vn, verbose: bool = True) -> None:
    """Entrena reglas y ejemplos del dominio llamadas."""
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
