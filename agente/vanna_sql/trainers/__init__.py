"""Agregador de trainers por dominio.

Orden recomendado:
  base → clientes → pedidos → llamadas → rutas → puntos → productos → mapas

Uso rápido:
    from agente.vanna_sql.trainers import train_all
    train_all(vn)

Por dominio:
    from agente.vanna_sql.trainers import train_domain
    train_domain(vn, "pedidos")
"""

from __future__ import annotations

from agente.vanna_sql.trainers import (
    trainer_base,
    trainer_clientes,
    trainer_pedidos,
    trainer_llamadas,
    trainer_rutas,
    trainer_puntos,
    trainer_productos,
    trainer_mapas,
)

# Orden canónico de entrenamiento: base siempre primero
DOMINIOS: list[str] = [
    "base",
    "clientes",
    "pedidos",
    "llamadas",
    "rutas",
    "puntos",
    "productos",
    "mapas",
]

_TRAINER_MAP = {
    "base":      trainer_base,
    "clientes":  trainer_clientes,
    "pedidos":   trainer_pedidos,
    "llamadas":  trainer_llamadas,
    "rutas":     trainer_rutas,
    "puntos":    trainer_puntos,
    "productos": trainer_productos,
    "mapas":     trainer_mapas,
}


def train_domain(vn, dominio: str, verbose: bool = True) -> None:
    """Entrena un único dominio.

    Args:
        vn:      Instancia de AtlasVanna ya inicializada.
        dominio: Nombre del dominio (ver DOMINIOS).
        verbose: Imprime progreso si True.

    Raises:
        ValueError: Si el dominio no existe.
    """
    trainer = _TRAINER_MAP.get(dominio)
    if trainer is None:
        opciones = ", ".join(DOMINIOS)
        raise ValueError(f"Dominio desconocido: '{dominio}'. Opciones: {opciones}")
    trainer.train(vn, verbose=verbose)


def train_all(vn, verbose: bool = True) -> None:
    """Entrena todos los dominios en orden canónico.

    Siempre ejecuta 'base' primero (DDL + reglas globales), luego el resto.

    Args:
        vn:      Instancia de AtlasVanna ya inicializada.
        verbose: Imprime progreso si True.
    """
    total = len(DOMINIOS)
    for i, dominio in enumerate(DOMINIOS, 1):
        if verbose:
            print(f"\n{'='*60}")
            print(f"[{i}/{total}] Dominio: {dominio.upper()}")
            print(f"{'='*60}")
        train_domain(vn, dominio, verbose=verbose)

    if verbose:
        print(f"\n{'='*60}")
        print(f"✔ train_all completo — {total} dominios entrenados")
        print(f"{'='*60}")
