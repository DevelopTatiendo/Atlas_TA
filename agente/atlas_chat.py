"""Componente Streamlit del chat Atlas Agent.

Expone render_chat_tab(ciudad) para incrustar en app.py como una pestaña.
Gestiona el historial de mensajes, el ciclo pregunta→respuesta y el botón
de mapa cuando el agente genera uno.

Uso en app.py:
    from agente.atlas_chat import render_chat_tab
    tab1, tab2 = st.tabs(["🗺️ Mapa de Muestras", "🤖 Atlas Agent"])
    with tab2:
        render_chat_tab(ciudad)
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

import streamlit as st

# URL base del servidor Flask que sirve los mapas estáticos
FLASK_SERVER = os.getenv("FLASK_SERVER_URL", "http://localhost:5000")

# Mensaje de bienvenida mostrado al abrir el chat por primera vez
_BIENVENIDA = """\
Hola, soy **Atlas Agent** 🗺️

Puedo generarte mapas en lenguaje natural. Ejemplos:

- *"Mapa de cobertura de la ruta Laureles en Medellín"*
- *"Clientes con deuda vencida en Cali"*
- *"Calor de visitas en Bogotá en abril"*
- *"Clientes activos en Barranquilla este mes"*

¿Qué mapa quieres ver?
"""

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_agent():
    """Retorna (o crea) la instancia AtlasAgent guardada en session_state."""
    if "atlas_agent_instance" not in st.session_state:
        from agente.atlas_agent import AtlasAgent
        st.session_state["atlas_agent_instance"] = AtlasAgent()
    return st.session_state["atlas_agent_instance"]


def _html_path_a_url(html_path: str) -> str:
    """Convierte ruta absoluta del mapa a URL pública del servidor Flask."""
    fname = Path(html_path).name
    ts    = int(time.time())
    return f"{FLASK_SERVER}/static/maps/{fname}?t={ts}"


def _init_historial():
    """Inicializa el historial con el mensaje de bienvenida si está vacío."""
    if "atlas_chat_history" not in st.session_state:
        st.session_state["atlas_chat_history"] = [
            {"role": "assistant", "content": _BIENVENIDA, "mapa_url": None}
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Componente principal
# ─────────────────────────────────────────────────────────────────────────────

def render_chat_tab(ciudad: str) -> None:
    """Renderiza la pestaña del chat Atlas Agent.

    Args:
        ciudad: Ciudad seleccionada en el sidebar (se muestra como contexto
                activo para el usuario, pero el agente puede aceptar otra ciudad
                en el mensaje).
    """
    _init_historial()

    # ── Cabecera ──────────────────────────────────────────────────────────────
    col_info, col_reset = st.columns([5, 1])
    with col_info:
        st.caption(
            f"🌆 Ciudad en sidebar: **{ciudad}** — puedes mencionarla o usar otra en tu consulta."
        )
    with col_reset:
        if st.button("🔄 Reiniciar", help="Limpia el historial y empieza una sesión nueva"):
            st.session_state["atlas_chat_history"] = []
            if "atlas_agent_instance" in st.session_state:
                st.session_state["atlas_agent_instance"].limpiar_historial()
            _init_historial()
            st.rerun()

    st.markdown(
        "<div style='height:4px;border-top:1px solid var(--border,#E5E7EB);margin:4px 0 12px 0;'></div>",
        unsafe_allow_html=True,
    )

    # ── Historial de mensajes ─────────────────────────────────────────────────
    for msg in st.session_state["atlas_chat_history"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("mapa_url"):
                st.link_button("🗺️ Ver Mapa", msg["mapa_url"], type="primary")

    # ── Input del usuario ─────────────────────────────────────────────────────
    if prompt := st.chat_input("¿Qué mapa quieres ver?", key="atlas_chat_input"):

        # Mostrar mensaje del usuario
        with st.chat_message("user"):
            st.markdown(prompt)
        st.session_state["atlas_chat_history"].append(
            {"role": "user", "content": prompt, "mapa_url": None}
        )

        # Llamar al agente
        agent = _get_agent()
        with st.chat_message("assistant"):
            indicadores = st.empty()
            indicadores.markdown("*Analizando…* ⏳")

            respuesta = agent.preguntar(prompt)

            indicadores.empty()

            # Recuperar mapa generado (si hubo)
            mapa_url = None
            if agent._ultimo_mapa:
                mapa_url = _html_path_a_url(agent._ultimo_mapa)
                agent._ultimo_mapa = None  # consumido

            st.markdown(respuesta)
            if mapa_url:
                st.link_button("🗺️ Ver Mapa", mapa_url, type="primary")

        # Guardar en historial
        st.session_state["atlas_chat_history"].append(
            {"role": "assistant", "content": respuesta, "mapa_url": mapa_url}
        )
