"""Trainer base: todos los DDL + reglas comunes de CO, id_contacto y protocolo.

Debe ejecutarse PRIMERO en cualquier entrenamiento (completo o por dominio).
Entrena los 23 bloques DDL del esquema completo más las 5 reglas fundamentales.
"""

from __future__ import annotations
from agente.vanna_sql.trainer import DDL_BLOCKS, DOC_BLOCKS

DOMINIO = "base"

DDL    = DDL_BLOCKS          # todos los DDL del esquema
DOCS   = [DOC_BLOCKS[i] for i in [0, 1, 2, 15, 16]]
# DOC[0]  = CO obligatorio
# DOC[1]  = id_contacto alias
# DOC[2]  = protocolo COUNT previo
# DOC[15] = relaciones confirmadas entre tablas
# DOC[16] = protocolo de clarificación


def train(vn, verbose: bool = True) -> None:
    """Entrena DDL completo + reglas base. Punto de partida obligatorio."""
    def log(msg):
        if verbose: print(msg)

    log(f"\n=== [{DOMINIO}] DDL — {len(DDL)} tablas/vistas ===")
    for i, ddl in enumerate(DDL, 1):
        vn.train(ddl=ddl)
        log(f"  [DDL {i:02d}/{len(DDL)}] OK")

    log(f"\n=== [{DOMINIO}] Docs — {len(DOCS)} reglas comunes ===")
    for i, doc in enumerate(DOCS, 1):
        vn.train(documentation=doc)
        log(f"  [DOC {i}] OK")

    log(f"\n✔ [{DOMINIO}] completo — DDL:{len(DDL)} Docs:{len(DOCS)}")
