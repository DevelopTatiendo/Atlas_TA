"""Captura de screenshots de mapas Folium usando Playwright.

Requiere instalación previa:
    pip install playwright
    playwright install chromium

Uso típico:
    from agente.captura import capturar_mapa_html
    png_path = capturar_mapa_html("static/maps/mapa_cali.html")
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path


# ── Parámetros por defecto ────────────────────────────────────────────────────

VIEWPORT_DEFAULT = {"width": 1400, "height": 900}

# Tiempo de espera para que carguen tiles y JS de capas (ms)
DELAY_CARGA_MS = 2500


# ── Función principal (async) ─────────────────────────────────────────────────

async def _capturar_async(
    html_path: str,
    output_png: str,
    viewport: dict,
    delay_ms: int,
) -> str:
    """Abre el HTML en Chromium headless y toma screenshot."""
    from playwright.async_api import async_playwright

    html_abs = str(Path(html_path).resolve())
    url = f"file:///{html_abs}".replace("\\", "/")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport=viewport)
        await page.goto(url)

        # Esperar a que el mapa termine de renderizar (tiles + cascade JS)
        await page.wait_for_timeout(delay_ms)

        # Screenshot del viewport visible (no full_page para no capturar scroll)
        await page.screenshot(path=output_png, full_page=False)
        await browser.close()

    return output_png


# ── Función pública (síncrona, para llamar desde código no-async) ─────────────

def capturar_mapa_html(
    html_path: str,
    output_png: str | None = None,
    viewport: dict | None = None,
    delay_ms: int = DELAY_CARGA_MS,
) -> str:
    """Captura screenshot de un archivo HTML de mapa Folium.

    Args:
        html_path:  Ruta al .html generado por generar_mapa_muestras_visual.
        output_png: Ruta destino del PNG. Si es None, se usa el mismo nombre que el HTML.
        viewport:   Dict con 'width' y 'height' del navegador simulado.
        delay_ms:   Milisegundos de espera tras cargar la página (tiles + JS).

    Returns:
        Ruta absoluta al PNG generado.

    Raises:
        FileNotFoundError: Si html_path no existe.
        ImportError: Si playwright no está instalado.
    """
    if not os.path.exists(html_path):
        raise FileNotFoundError(f"HTML no encontrado: {html_path}")

    if output_png is None:
        output_png = str(Path(html_path).with_suffix(".png"))

    if viewport is None:
        viewport = VIEWPORT_DEFAULT

    # Asegurar que el directorio destino existe
    Path(output_png).parent.mkdir(parents=True, exist_ok=True)

    return asyncio.run(_capturar_async(html_path, output_png, viewport, delay_ms))


# ── Captura múltiple (batch) ──────────────────────────────────────────────────

def capturar_mapas_batch(
    html_paths: list[str],
    output_dir: str | None = None,
    delay_ms: int = DELAY_CARGA_MS,
) -> list[str]:
    """Captura screenshots de múltiples mapas HTML.

    Args:
        html_paths: Lista de rutas a archivos HTML.
        output_dir: Directorio donde guardar los PNGs.
                    Si es None, cada PNG queda junto a su HTML.

    Returns:
        Lista de rutas a los PNGs generados.
    """
    resultados = []
    for html_path in html_paths:
        if output_dir:
            nombre = Path(html_path).stem + ".png"
            png_path = str(Path(output_dir) / nombre)
        else:
            png_path = None

        try:
            png = capturar_mapa_html(html_path, png_path, delay_ms=delay_ms)
            resultados.append(png)
        except Exception as e:
            print(f"[captura] Error en {html_path}: {e}")
            resultados.append(None)

    return resultados
