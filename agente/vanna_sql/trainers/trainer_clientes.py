"""Trainer dominio: clientes.

Cubre: cliente activo, canal directo, categoría, barrio, subestado,
consulta base de clientes, catálogo de cargos.
"""

from __future__ import annotations
from agente.vanna_sql.trainer import DOC_BLOCKS, SQL_EXAMPLES

DOMINIO = "clientes"

DOCS = [DOC_BLOCKS[i] for i in [3, 4, 8, 12]]
# DOC[3]  = canal directo (id_canal=2)
# DOC[4]  = cliente activo (estado_cxc, cant_obsequios)
# DOC[8]  = consulta base de clientes (plantilla completa)
# DOC[12] = catálogo de cargos

EXAMPLES = [SQL_EXAMPLES[i] for i in [0, 6, 7]]
# SQL[0]  = base activa completa para mapa (CO Medellín)
# SQL[6]  = clientes por barrio específico
# SQL[7]  = clientes por categoría específica


def train(vn, verbose: bool = True) -> None:
    """Entrena reglas y ejemplos del dominio clientes."""
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
