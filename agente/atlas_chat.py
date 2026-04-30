"""Componente Streamlit del chat Atlas Agent.

Flujo directo:
  1. Usuario describe qué clientes quiere ver.
  2. Agente llama consultar_clientes → si hay coords → llama generar_mapa_clientes.
  3. UI renderiza KPI cards + botón "Ver Mapa" automáticamente.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import streamlit as st

FLASK_SERVER = os.getenv("FLASK_SERVER_URL", "http://localhost:5000")

_BIENVENIDA = """\
Hola, soy **Atlas Agent** 🗺️

Dime qué clientes quieres ver en el mapa. Ejemplos:

- *"Clientes activos de Medellín con pedido válido en abril 2026"*
- *"No-fieles de Cali sin visita en lo que va de 2026"*
- *"Clientes de Bogotá con pedido de BluePet en el primer trimestre 2026"*
- *"Clientes con deuda mayor a 50 mil en Barranquilla este mes"*
- *"Cobertura de la ruta Aranjuez en Medellín desde enero 2026"*
- *"Clientes con más de 500 puntos acumulados en Pereira"*
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_agent():
    if "atlas_agent_instance" not in st.session_state:
        from agente.atlas_agent import AtlasAgent
        st.session_state["atlas_agent_instance"] = AtlasAgent()
    return st.session_state["atlas_agent_instance"]


def _html_path_a_url(html_path: str) -> str:
    fname = Path(html_path).name
    ts    = int(time.time())
    return f"{FLASK_SERVER}/static/maps/{fname}?t={ts}"


def _init_historial():
    if "atlas_chat_history" not in st.session_state:
        st.session_state["atlas_chat_history"] = [
            {"role": "assistant", "content": _BIENVENIDA,
             "mapa_url": None, "consulta": None}
        ]


# ─────────────────────────────────────────────────────────────────────────────
# KPI rendering
# ─────────────────────────────────────────────────────────────────────────────

def _render_kpi_cards(consulta: dict, expandido: bool = True) -> None:
    """Renderiza métricas automáticas del resultado de consultar_clientes."""
    kpis    = consulta.get("kpis", [])
    muestra = consulta.get("muestra", [])
    titulo  = consulta.get("titulo", "")

    if titulo:
        st.caption(f"📊 {titulo}")

    resumen = [k for k in kpis if k.get("grupo") == "resumen"]
    resto   = [k for k in kpis if k.get("grupo") != "resumen"]

    # Métricas de resumen: total, coords, %
    if resumen:
        cols = st.columns(len(resumen))
        for col, k in zip(cols, resumen):
            col.metric(k["nombre"], k["valor_fmt"])

    # KPIs por columna
    if resto:
        grupos: dict[str, list] = {}
        for k in resto:
            grupos.setdefault(k.get("grupo", ""), []).append(k)
        for _, items in grupos.items():
            n = min(len(items), 4)
            cols = st.columns(n)
            for i, k in enumerate(items[:n]):
                cols[i].metric(k["nombre"], k["valor_fmt"])

    # Muestra de datos
    if muestra and expandido:
        import pandas as pd
        with st.expander(f"👁️ Primeras {len(muestra)} filas", expanded=False):
            st.dataframe(pd.DataFrame(muestra), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# Componente principal
# ─────────────────────────────────────────────────────────────────────────────

def render_chat_tab(ciudad: str) -> None:
    _init_historial()

    # Cabecera
    col_info, col_reset = st.columns([5, 1])
    with col_info:
        st.caption(f"🌆 Ciudad activa: **{ciudad}** — menciónala o usa otra en tu consulta.")
    with col_reset:
        if st.button("🔄 Reiniciar", help="Nueva sesión"):
            st.session_state["atlas_chat_history"] = []
            if "atlas_agent_instance" in st.session_state:
                st.session_state["atlas_agent_instance"].limpiar_historial()
            _init_historial()
            st.rerun()

    st.markdown(
        "<div style='height:4px;border-top:1px solid var(--border,#E5E7EB);margin:4px 0 12px 0;'></div>",
        unsafe_allow_html=True,
    )

    # Historial
    for msg in st.session_state["atlas_chat_history"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("consulta"):
                _render_kpi_cards(msg["consulta"], expandido=False)
            if msg.get("mapa_url"):
                st.link_button("🗺️ Ver Mapa", msg["mapa_url"], type="primary")

    # Input
    if prompt := st.chat_input("¿Qué mapa quieres ver?", key="atlas_chat_input"):
        _procesar_mensaje(prompt, _get_agent())
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Procesamiento
# ─────────────────────────────────────────────────────────────────────────────

def _procesar_mensaje(prompt: str, agent) -> None:
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state["atlas_chat_history"].append(
        {"role": "user", "content": prompt, "mapa_url": None, "consulta": None}
    )

    agent._ultima_consulta = None

    with st.chat_message("assistant"):
        indicadores = st.empty()
        indicadores.markdown("*Analizando…* ⏳")

        respuesta = agent.preguntar(prompt)

        indicadores.empty()

        mapa_url      = None
        consulta_data = None

        if agent._ultimo_mapa:
            mapa_url          = _html_path_a_url(agent._ultimo_mapa)
            agent._ultimo_mapa = None

        if agent._ultima_consulta:
            consulta_data = agent._ultima_consulta

        st.markdown(respuesta)

        if consulta_data:
            _render_kpi_cards(consulta_data, expandido=True)

        if mapa_url:
            agent._ultima_consulta = None
            st.link_button("🗺️ Ver Mapa", mapa_url, type="primary")

    st.session_state["atlas_chat_history"].append({
        "role":     "assistant",
        "content":  respuesta,
        "mapa_url": mapa_url,
        "consulta": consulta_data,
    })
