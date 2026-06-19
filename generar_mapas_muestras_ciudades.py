from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
MAPS_DIR = BASE_DIR / "static" / "maps"
DEFAULT_OUTPUT_DIR = MAPS_DIR / "muestras_ciudades"

CIUDADES = {
    "BARRANQUILLA": "Barranquilla",
    "BOGOTA": "Bogota",
    "BUCARAMANGA": "Bucaramanga",
    "CALI": "Cali",
    "MANIZALES": "Manizales",
    "MEDELLIN": "Medellin",
    "PEREIRA": "Pereira",
}

TIPOS_MAPA = {
    "MES": "Mes",
    "MESES": "Mes",
    "PROMOTOR": "Promotor",
    "PROMOTORES": "Promotor",
}


@dataclass
class ResultadoCiudad:
    ciudad: str
    ok: bool
    archivo: Path | None = None
    puntos: int = 0
    error: str | None = None


def _parse_fecha(valor: str, nombre: str) -> str:
    try:
        return datetime.strptime(valor.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{nombre} debe tener formato YYYY-MM-DD: {valor!r}") from exc


def _parse_tipo(valor: str) -> str:
    key = (valor or "").strip().upper()
    if key not in TIPOS_MAPA:
        opciones = ", ".join(sorted(TIPOS_MAPA))
        raise ValueError(f"Tipo de mapa no valido: {valor!r}. Opciones: {opciones}")
    return TIPOS_MAPA[key]


def _slug_ciudad(ciudad: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", ciudad.upper())


def _limpiar_mapa_previo_ciudad(output_dir: Path, ciudad_slug: str) -> None:
    for archivo in output_dir.glob(f"Mapa_Muestras_{ciudad_slug}*.html"):
        archivo.unlink()


def _guardar_unico_por_ciudad(
    filename_generado: str,
    ciudad: str,
    output_dir: Path,
) -> Path:
    origen = MAPS_DIR / filename_generado
    if not origen.exists():
        raise FileNotFoundError(f"No se encontro el mapa generado: {origen}")

    ciudad_slug = _slug_ciudad(ciudad)
    output_dir.mkdir(parents=True, exist_ok=True)
    _limpiar_mapa_previo_ciudad(output_dir, ciudad_slug)

    destino = output_dir / f"Mapa_Muestras_{ciudad_slug}.html"
    shutil.copy2(origen, destino)
    return destino


def _cargar_generador_muestras():
    from config.secrets_manager import load_env_secure

    load_env_secure(
        prefer_plain=True,
        enc_path=str(BASE_DIR / "config" / ".env.enc"),
        pass_env_var="MAPAS_SECRET_PASSPHRASE",
        cache=False,
    )

    from mapa_muestras import CENTROOPES, generar_mapa_muestras_visual

    return CENTROOPES, generar_mapa_muestras_visual


def generar_mapas_todas_las_ciudades(
    fecha_inicio: str,
    fecha_fin: str,
    tipo_mapa: str,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> list[ResultadoCiudad]:
    agrupar_por = _parse_tipo(tipo_mapa)
    fecha_inicio = _parse_fecha(fecha_inicio, "fecha_inicio")
    fecha_fin = _parse_fecha(fecha_fin, "fecha_fin")

    if fecha_inicio > fecha_fin:
        raise ValueError("fecha_inicio no puede ser mayor que fecha_fin")

    os.chdir(BASE_DIR)
    centroopes, generar_mapa_muestras_visual = _cargar_generador_muestras()
    resultados: list[ResultadoCiudad] = []
    ciudades = [CIUDADES[k] for k in CIUDADES if k in centroopes]

    print(f"Generando mapas de muestras por {agrupar_por}")
    print(f"Rango: {fecha_inicio} a {fecha_fin}")
    print(f"Salida: {output_dir}")
    print("-" * 70)

    for ciudad in ciudades:
        try:
            print(f"[{ciudad}] Generando...")
            filename, n_puntos, _df_export = generar_mapa_muestras_visual(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                ciudad=ciudad,
                agrupar_por=agrupar_por,
                auditoria=False,
                override_fc=None,
            )
            destino = _guardar_unico_por_ciudad(filename, ciudad, output_dir)
            resultados.append(
                ResultadoCiudad(
                    ciudad=ciudad,
                    ok=True,
                    archivo=destino,
                    puntos=int(n_puntos or 0),
                )
            )
            print(f"[{ciudad}] OK -> {destino.name} ({int(n_puntos or 0)} puntos)")
        except Exception as exc:
            resultados.append(ResultadoCiudad(ciudad=ciudad, ok=False, error=str(exc)))
            print(f"[{ciudad}] ERROR -> {exc}")

    return resultados


def _pedir_si_falta(valor: str | None, etiqueta: str) -> str:
    if valor:
        return valor
    return input(f"{etiqueta}: ").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un mapa de muestras por cada ciudad usando el mismo flujo de app.py."
    )
    parser.add_argument("--fecha-inicio", help="Fecha inicial en formato YYYY-MM-DD")
    parser.add_argument("--fecha-fin", help="Fecha final en formato YYYY-MM-DD")
    parser.add_argument(
        "--tipo-mapa",
        help="Tipo de mapa: MES o PROMOTOR",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Carpeta donde se guardara un unico HTML por ciudad.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        fecha_inicio = _pedir_si_falta(args.fecha_inicio, "Fecha de inicio (YYYY-MM-DD)")
        fecha_fin = _pedir_si_falta(args.fecha_fin, "Fecha fin (YYYY-MM-DD)")
        tipo_mapa = _pedir_si_falta(args.tipo_mapa, "Tipo de mapa (MES o PROMOTOR)")

        resultados = generar_mapas_todas_las_ciudades(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            tipo_mapa=tipo_mapa,
            output_dir=Path(args.output_dir).resolve(),
        )

        exitosos = [r for r in resultados if r.ok]
        fallidos = [r for r in resultados if not r.ok]

        print("-" * 70)
        print(f"Mapas generados: {len(exitosos)}")
        print(f"Errores: {len(fallidos)}")
        for r in exitosos:
            print(f"OK {r.ciudad}: {r.archivo} ({r.puntos} puntos)")
        for r in fallidos:
            print(f"ERROR {r.ciudad}: {r.error}")

        return 1 if fallidos else 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
