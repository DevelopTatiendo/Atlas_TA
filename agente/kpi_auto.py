"""Cálculo automático de KPIs a partir de un DataFrame de clientes.

No requiere que el agente sepa qué tipo de consulta se hizo.
Detecta el tipo de cada columna y calcula las métricas relevantes.

Reglas de detección:
  - Columnas monetarias (deuda, saldo, valor, monto, total, cobro, factura, resta, pagos)
      → suma, promedio, máximo, cantidad > 0
  - Columnas de proporción/flag (visitado, activo, convertido, fiel, vencida, es_*)
      → conteo, porcentaje sobre total
  - Columnas de fecha/edad (fecha_*, ultima_*, edad_*, dias_*)
      → mín, máx, promedio, cuántos nulos
  - Columnas de conteo (cant_*, n_*, num_*, cantidad*)
      → suma, promedio, máximo
  - Siempre: n_total, n_con_coords, pct_cobertura_geo

Retorna:
  Lista de dicts { nombre, valor, fmt }
  fmt ∈ "entero" | "moneda" | "porcentaje" | "dias" | "fecha" | "decimal"
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Patrones de nombre de columna → tipo
# ─────────────────────────────────────────────────────────────────────────────

_MONETARIAS = re.compile(
    r"(deuda|saldo|valor|monto|total|cobro|factura|resta|pagos|ingreso|venta|precio|importe)",
    re.I,
)
_FLAGS = re.compile(
    r"^(visitado|activo|convertido|fiel|vencid[ao]|es_|tiene_|con_|sin_)",
    re.I,
)
_FECHAS = re.compile(
    r"(fecha|ultima_|ultimo_|edad_|dias_|antiguedad)",
    re.I,
)
_CONTEOS = re.compile(
    r"^(cant_|n_|num_|cantidad|count_|total_)",
    re.I,
)

# Columnas técnicas que nunca son KPI
_EXCLUIR = {"id_contacto", "id", "lat", "lon", "nombre", "barrio", "ruta",
            "ruta_cobro", "ciudad", "id_barrio", "id_ciudad", "id_ruta",
            "n_eventos_con_coords"}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de formato
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_cop(v: float) -> str:
    if abs(v) >= 1_000_000_000:
        return f"${v/1_000_000_000:.1f}B"
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"


def _fmt_valor(v, fmt: str) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    if fmt == "moneda":
        return _fmt_cop(float(v))
    if fmt == "porcentaje":
        return f"{float(v):.1f}%"
    if fmt == "entero":
        return f"{int(v):,}"
    if fmt == "decimal":
        return f"{float(v):.1f}"
    if fmt == "dias":
        return f"{int(v):,} días"
    return str(v)


# ─────────────────────────────────────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────────────────────────────────────

def calcular_kpis(
    df: pd.DataFrame,
    n_con_coords: int,
    excluir_cols: list[str] | None = None,
) -> list[dict]:
    """Calcula KPIs automáticos a partir de un DataFrame de clientes.

    Args:
        df:            DataFrame con los clientes del filtro (con lat/lon ya merged).
        n_con_coords:  Cuántos de esos clientes tienen coordenadas válidas.
        excluir_cols:  Columnas adicionales a excluir del análisis.

    Returns:
        Lista ordenada de dicts: { nombre, valor, valor_fmt, fmt, grupo }
    """
    excluir = _EXCLUIR | set(excluir_cols or [])
    n_total = len(df)
    kpis: list[dict] = []

    # ── KPIs fijos de cobertura ───────────────────────────────────────────────
    kpis.append({
        "nombre": "Clientes encontrados",
        "valor": n_total,
        "valor_fmt": f"{n_total:,}",
        "fmt": "entero",
        "grupo": "resumen",
    })
    kpis.append({
        "nombre": "Con coordenadas GPS",
        "valor": n_con_coords,
        "valor_fmt": f"{n_con_coords:,}",
        "fmt": "entero",
        "grupo": "resumen",
    })
    pct_geo = round(100 * n_con_coords / n_total, 1) if n_total else 0
    kpis.append({
        "nombre": "Cobertura GPS",
        "valor": pct_geo,
        "valor_fmt": f"{pct_geo:.1f}%",
        "fmt": "porcentaje",
        "grupo": "resumen",
    })

    # ── KPIs por columna ──────────────────────────────────────────────────────
    for col in df.columns:
        if col in excluir:
            continue

        col_lower = col.lower()
        serie = df[col]

        # — Columnas monetarias —
        if _MONETARIAS.search(col_lower):
            num = pd.to_numeric(serie, errors="coerce").dropna()
            if num.empty:
                continue
            label = col.replace("_", " ").title()
            kpis += [
                {"nombre": f"{label} — Total",    "valor": num.sum(),    "valor_fmt": _fmt_cop(num.sum()),    "fmt": "moneda",  "grupo": col},
                {"nombre": f"{label} — Promedio", "valor": num.mean(),   "valor_fmt": _fmt_cop(num.mean()),   "fmt": "moneda",  "grupo": col},
                {"nombre": f"{label} — Máximo",   "valor": num.max(),    "valor_fmt": _fmt_cop(num.max()),    "fmt": "moneda",  "grupo": col},
                {"nombre": f"Con {label} > 0",    "valor": int((num > 0).sum()), "valor_fmt": f"{int((num>0).sum()):,}", "fmt": "entero", "grupo": col},
            ]
            continue

        # — Columnas flag / proporción —
        if _FLAGS.search(col_lower):
            num = pd.to_numeric(serie, errors="coerce").dropna()
            if num.empty or set(num.unique()) - {0, 1, True, False}:
                continue
            cnt = int(num.sum())
            pct = round(100 * cnt / n_total, 1) if n_total else 0
            label = col.replace("_", " ").title()
            kpis += [
                {"nombre": f"{label}",            "valor": cnt,  "valor_fmt": f"{cnt:,}", "fmt": "entero",     "grupo": col},
                {"nombre": f"% {label}",          "valor": pct,  "valor_fmt": f"{pct:.1f}%", "fmt": "porcentaje", "grupo": col},
            ]
            continue

        # — Columnas de edad/días (numéricas) —
        if _FECHAS.search(col_lower) and not col_lower.startswith("fecha"):
            num = pd.to_numeric(serie, errors="coerce").dropna()
            if num.empty:
                continue
            label = col.replace("_", " ").title()
            kpis += [
                {"nombre": f"{label} — Promedio", "valor": round(num.mean(), 0), "valor_fmt": f"{num.mean():.0f} días", "fmt": "dias", "grupo": col},
                {"nombre": f"{label} — Máximo",   "valor": int(num.max()),       "valor_fmt": f"{int(num.max()):,} días", "fmt": "dias", "grupo": col},
            ]
            continue

        # — Columnas de fecha (datetime/string) —
        if _FECHAS.search(col_lower) and col_lower.startswith("fecha"):
            try:
                fechas = pd.to_datetime(serie, errors="coerce").dropna()
                if fechas.empty:
                    continue
                label = col.replace("_", " ").title()
                kpis += [
                    {"nombre": f"{label} — Más reciente", "valor": str(fechas.max().date()), "valor_fmt": str(fechas.max().date()), "fmt": "fecha", "grupo": col},
                    {"nombre": f"{label} — Más antigua",  "valor": str(fechas.min().date()), "valor_fmt": str(fechas.min().date()), "fmt": "fecha", "grupo": col},
                ]
            except Exception:
                pass
            continue

        # — Columnas de conteo —
        if _CONTEOS.search(col_lower):
            num = pd.to_numeric(serie, errors="coerce").dropna()
            if num.empty:
                continue
            label = col.replace("_", " ").title()
            kpis += [
                {"nombre": f"{label} — Total",    "valor": int(num.sum()),   "valor_fmt": f"{int(num.sum()):,}",  "fmt": "entero", "grupo": col},
                {"nombre": f"{label} — Promedio", "valor": round(num.mean(), 1), "valor_fmt": f"{num.mean():.1f}", "fmt": "decimal", "grupo": col},
            ]
            continue

    return kpis


def kpis_a_markdown(kpis: list[dict]) -> str:
    """Convierte lista de KPIs a tabla markdown para el agente."""
    if not kpis:
        return ""
    lineas = ["| Métrica | Valor |", "|---------|-------|"]
    for k in kpis:
        lineas.append(f"| {k['nombre']} | {k['valor_fmt']} |")
    return "\n".join(lineas)
