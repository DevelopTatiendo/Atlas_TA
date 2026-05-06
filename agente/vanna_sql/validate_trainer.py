"""Validación estructural del entrenamiento Vanna.

Genera SQL para cada pregunta de prueba y verifica que contenga los fragmentos
obligatorios (must_contain) y que NO contenga los prohibidos (must_not_contain).

Uso como script:
    python -m agente.vanna_sql.validate_trainer --provider groq
    python -m agente.vanna_sql.validate_trainer --provider groq --domain pedidos

Cada resultado se escribe en agente/vanna_sql/validation_results.jsonl.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Casos de validación
# Cada caso define:
#   question        → pregunta en lenguaje natural
#   domain          → dominio al que pertenece (para filtrar con --domain)
#   must_contain    → lista de fragmentos SQL que DEBEN aparecer (case-insensitive)
#   must_not_contain → lista de fragmentos que NO deben aparecer (case-insensitive)
# ─────────────────────────────────────────────────────────────────────────────

VALIDATION_CASES: list[dict[str, Any]] = [
    # ── MAPAS ─────────────────────────────────────────────────────────────────
    {
        "question": "Clientes de Cali de la ruta 2 con pedidos válidos mayores a 200 mil pesos en abril 2026",
        "domain": "mapas",
        "must_contain": [
            "id_contacto",
            "id_centroope = 2",
            "rutas_cobro_zonas",
            "rutas_cobro",
            "estado_pedido = 1",
            "anulada = 0",
            "autorizar IN (1, 2)",
            "autorizacion_descuento = 0",
            "tipo_documento < 2",
            "HAVING",
            "200000",
        ],
        "must_not_contain": [
            r"\blat\b",
            r"\blon\b",
            "latitud",
            "longitud",
            "DELETE",
            "UPDATE",
            "DROP",
        ],
    },
    {
        "question": "Mapa de clientes de Medellín con score de pedidos y llamadas contestadas en Q1 2026",
        "domain": "mapas",
        "must_contain": [
            "id_contacto",
            "id_centroope = 3",
            "llamadas_respuestas",
            "id_respuesta",
            "lr.contestada = 1",
        ],
        "must_not_contain": [
            r"\blat\b",
            r"\blon\b",
            "ultima_llamada",
        ],
    },
    # ── LLAMADAS ──────────────────────────────────────────────────────────────
    {
        "question": "Clientes de Medellín con más de 3 llamadas contestadas en abril 2026",
        "domain": "llamadas",
        "must_contain": [
            "llamadas_respuestas",
            "id_respuesta",
            "lr.contestada = 1",
            "l.estado = 1",
            "id_centroope = 3",
        ],
        "must_not_contain": [
            "contactos.ultima_llamada",
            "DELETE",
            "UPDATE",
        ],
    },
    {
        "question": "Tasa de conversión a venta por vendedor en Bogotá en marzo 2026",
        "domain": "llamadas",
        "must_contain": [
            "llamadas_respuestas",
            "id_respuesta",
            "es_venta",
            "id_centroope = 4",
        ],
        "must_not_contain": [
            "ultima_llamada",
        ],
    },
    # ── PEDIDOS ───────────────────────────────────────────────────────────────
    {
        "question": "Clientes de Bogotá con pedido válido en febrero 2026 ordenados por valor",
        "domain": "pedidos",
        "must_contain": [
            "id_contacto",
            "id_centroope = 4",
            "estado_pedido = 1",
            "anulada = 0",
            "autorizar IN (1, 2)",
            "autorizacion_descuento = 0",
            "tipo_documento < 2",
            "2026-02",
        ],
        "must_not_contain": [
            r"\blat\b",
            r"\blon\b",
            "fullclean_cartera.pedidos",
        ],
    },
    {
        "question": "Top 10 clientes de Cali por valor total de pedidos en Q1 2026",
        "domain": "pedidos",
        "must_contain": [
            "id_contacto",
            "id_centroope = 2",
            "estado_pedido = 1",
            "valor_total",
            "ORDER BY",
            "LIMIT 10",
        ],
        "must_not_contain": [
            "fullclean_cartera",
        ],
    },
    # ── PRODUCTOS ─────────────────────────────────────────────────────────────
    {
        "question": "Clientes de Bogotá que compraron productos de la línea BLUE PET en 2026",
        "domain": "productos",
        "must_contain": [
            "id_contacto",
            "id_centroope = 4",
            "pedidos_det",
            "items",
            "lineas",
            "BLUE PET",
            "estado_pedido = 1",
        ],
        "must_not_contain": [
            r"\blat\b",
            r"\blon\b",
        ],
    },
    # ── CLIENTES ──────────────────────────────────────────────────────────────
    {
        "question": "Clientes activos de Pereira del canal directo en barrio El Centro",
        "domain": "clientes",
        "must_contain": [
            "id_contacto",
            "id_centroope = 5",
            "id_canal = 2",
        ],
        "must_not_contain": [
            r"\blat\b",
            r"\blon\b",
            "DELETE",
        ],
    },
    # ── RUTAS ─────────────────────────────────────────────────────────────────
    {
        "question": "Clientes activos de la ruta de cobro 13 en Medellín",
        "domain": "rutas",
        "must_contain": [
            "id_contacto",
            "id_centroope = 3",
            "rutas_cobro_zonas",
            "rutas_cobro",
        ],
        "must_not_contain": [
            r"\blat\b",
            r"\blon\b",
        ],
    },
    # ── PUNTOS ────────────────────────────────────────────────────────────────
    {
        "question": "Clientes de Medellín con movimientos de puntos en Q1 2026",
        "domain": "puntos",
        "must_contain": [
            "id_contacto",
            "id_centroope = 3",
            "contactos_movimientos",
        ],
        "must_not_contain": [
            r"\blat\b",
            r"\blon\b",
        ],
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Motor de validación
# ─────────────────────────────────────────────────────────────────────────────

_RESULTS_PATH = Path(__file__).parent / "validation_results.jsonl"


def _check_fragment(sql: str, fragment: str) -> bool:
    """Verifica si el fragmento (regex o literal) aparece en el SQL (case-insensitive)."""
    try:
        return bool(re.search(fragment, sql, re.IGNORECASE))
    except re.error:
        return fragment.lower() in sql.lower()


def validar_caso(sql: str, caso: dict[str, Any]) -> dict[str, Any]:
    """
    Valida un SQL contra los requisitos del caso.

    Returns:
        dict con keys: passed, missing, forbidden_found
    """
    sql_norm = sql or ""
    missing: list[str] = []
    forbidden: list[str] = []

    for fragment in caso.get("must_contain", []):
        if not _check_fragment(sql_norm, fragment):
            missing.append(fragment)

    for fragment in caso.get("must_not_contain", []):
        if _check_fragment(sql_norm, fragment):
            forbidden.append(fragment)

    return {
        "passed": not missing and not forbidden,
        "missing": missing,
        "forbidden_found": forbidden,
    }


def run_validation(
    vn,
    cases: list[dict] | None = None,
    domain: str | None = None,
    sleep_between: float = 4.0,
    verbose: bool = True,
) -> list[dict[str, Any]]:
    """
    Ejecuta la suite de validación contra una instancia de AtlasVanna.

    Args:
        vn:             Instancia de AtlasVanna inicializada.
        cases:          Lista de casos; si None usa VALIDATION_CASES.
        domain:         Si se especifica, filtra solo casos de ese dominio.
        sleep_between:  Segundos entre preguntas (evita rate limit Groq).
        verbose:        Imprime resultado por caso si True.

    Returns:
        Lista de resultados. Cada resultado es un dict con:
            question, domain, sql_generado, passed, missing, forbidden_found,
            error (si Vanna falló), timestamp.
    """
    if cases is None:
        cases = VALIDATION_CASES

    if domain:
        cases = [c for c in cases if c.get("domain") == domain]
        if not cases:
            raise ValueError(f"No hay casos para el dominio '{domain}'.")

    resultados: list[dict[str, Any]] = []
    total   = len(cases)
    passed  = 0
    failed  = 0

    if verbose:
        print(f"\n{'='*65}")
        print(f"  Validación Vanna — {total} caso(s)" + (f" [dominio: {domain}]" if domain else ""))
        print(f"{'='*65}")

    for i, caso in enumerate(cases, 1):
        question = caso["question"]
        dom      = caso.get("domain", "?")

        if verbose:
            print(f"\n[{i}/{total}] [{dom}] {question[:70]}")

        resultado: dict[str, Any] = {
            "question":       question,
            "domain":         dom,
            "sql_generado":   None,
            "passed":         False,
            "missing":        [],
            "forbidden_found": [],
            "error":          None,
            "timestamp":      datetime.utcnow().isoformat(),
        }

        try:
            sql = vn.generate_sql(question)
            resultado["sql_generado"] = sql

            check = validar_caso(sql, caso)
            resultado.update(check)

            if check["passed"]:
                passed += 1
                if verbose:
                    print(f"  ✔ PASS")
            else:
                failed += 1
                if verbose:
                    print(f"  ✘ FAIL")
                    for m in check["missing"]:
                        print(f"      Falta:     {m}")
                    for f in check["forbidden_found"]:
                        print(f"      Prohibido: {f}")

        except Exception as exc:
            failed += 1
            resultado["error"] = str(exc)
            if verbose:
                print(f"  ✘ ERROR — {exc}")

        resultados.append(resultado)

        # Guardar resultado parcial en JSONL
        _guardar_resultado(resultado)

        # Pausa anti rate-limit
        if i < total:
            time.sleep(sleep_between)

    if verbose:
        print(f"\n{'='*65}")
        print(f"  Resultado: {passed} PASS  |  {failed} FAIL  |  {total} total")
        print(f"  Resultados → {_RESULTS_PATH}")
        print(f"{'='*65}\n")

    return resultados


def _guardar_resultado(resultado: dict[str, Any]) -> None:
    """Añade el resultado al archivo JSONL de resultados."""
    _RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _RESULTS_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(resultado, ensure_ascii=False) + "\n")


def resumen_resultados(resultados: list[dict[str, Any]]) -> dict[str, Any]:
    """Devuelve métricas agregadas de una corrida de validación."""
    total  = len(resultados)
    passed = sum(1 for r in resultados if r["passed"])
    failed = total - passed
    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "pct_pass": round(100 * passed / total, 1) if total else 0,
        "fallos": [
            {
                "question": r["question"],
                "domain": r["domain"],
                "missing": r["missing"],
                "forbidden_found": r["forbidden_found"],
                "error": r["error"],
            }
            for r in resultados
            if not r["passed"]
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Valida el entrenamiento Vanna generando SQL y chequeando contratos."
    )
    p.add_argument("--provider", default=None, help="gemini | groq (default: auto)")
    p.add_argument("--model",    default=None, help="Modelo LLM a usar")
    p.add_argument("--domain",   default=None, help="Filtrar por dominio")
    p.add_argument("--sleep",    type=float, default=4.0, help="Segundos entre preguntas (default: 4)")
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()

    from config.secrets_manager import load_env_secure
    load_env_secure()

    from agente.vanna_sql.atlas_vanna import get_vanna
    vn = get_vanna(model=args.model, provider=args.provider, connect_db=False)

    resultados = run_validation(
        vn,
        domain=args.domain,
        sleep_between=args.sleep,
    )
    res = resumen_resultados(resultados)
    print(json.dumps(res, ensure_ascii=False, indent=2))
