import datetime as dt
import os
import sys
import pandas as pd
from dotenv import load_dotenv


# Asegurar que el directorio raíz del repo esté en sys.path para imports relativos
CURR_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURR_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from new_mapa_muestras import generar_mapa_muestras


def main():
    # 1) Cargar variables de entorno desde el .env del proyecto
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    dotenv_path = os.path.join(project_root, ".env")
    load_dotenv(dotenv_path=dotenv_path)
    # print("DB_HOST:", os.getenv("DB_HOST"))  # debug opcional
    # Parámetros de prueba
    fecha_inicio = "2025-01-01"
    fecha_fin = dt.date.today().strftime("%Y-%m-%d")
    ciudad = "CALI"
    agrupar_por = "Mes"  # más adelante probamos "Mes"

    print("=== TEST new_mapa_muestras (modo clientes) ===")
    print(f"Ciudad: {ciudad} | Rango: {fecha_inicio} → {fecha_fin} | agrupar_por: {agrupar_por}")

    df_original, df_filtrado, df_agrupado = generar_mapa_muestras(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        ciudad=ciudad,
        agrupar_por=agrupar_por,
    )

    print("\n=== Tamaños de DataFrames ===")
    print(f"df_original: {df_original.shape}")
    print(f"df_filtrado: {df_filtrado.shape}")
    print(f"df_agrupado: {df_agrupado.shape}")

    if df_original.empty:
        print("\nNo se obtuvieron muestras en el rango de fechas.")
        return

    print("\n=== Columnas disponibles en df_agrupado ===")
    print(list(df_agrupado.columns))

    # Columnas clave para la leyenda detalle
    if agrupar_por == "Promotor":
        columnas_leyenda = [
            "id_promotor",
            "apellido_promotor",
            "muestras_total",
            "clientes_total",
            "dias_habiles",
            "clientes_por_dia_habil",
            "clientes_no_fieles",
            "clientes_contactables",
            "clientes_contactables_no_fieles",
            "pct_clientes_no_fieles",
            "pct_total_muestras_contactables",
            "pct_contactabilidad_no_fieles",
            "area_m2",
        ]
    else:  # agrupar_por == "Mes"
        columnas_leyenda = [
            "mes",
            "muestras_total",
            "clientes_total",
            "dias_habiles",
            "clientes_por_dia_habil",
            "clientes_no_fieles",
            "clientes_contactables",
            "clientes_contactables_no_fieles",
            "pct_clientes_no_fieles",
            "pct_total_muestras_contactables",
            "pct_contactabilidad_no_fieles",
            "area_m2",
            "clientes_por_area_m2",
        ]
    cols_presentes = [c for c in columnas_leyenda if c in df_agrupado.columns]

    print("\n=== Preview leyenda detalle (primeros 20 registros) ===")
    if cols_presentes:
        df_preview = df_agrupado.copy()
        # Para vista por mes, calcular clientes_por_area_m2 para mostrarlo en preview si aplica
        if agrupar_por == "Mes" and "area_m2" in df_preview.columns and "clientes_total" in df_preview.columns:
            df_preview["clientes_por_area_m2"] = df_preview.apply(
                lambda r: (r["clientes_total"] / r["area_m2"]) if (pd.notna(r.get("area_m2")) and r.get("area_m2") not in [None, 0]) else None,
                axis=1,
            )
        df_preview = df_preview[cols_presentes]
        if "clientes_total" in df_preview.columns:
            df_preview = df_preview.sort_values("clientes_total", ascending=False)
        print(df_preview.head(20).to_string(index=False))
    else:
        print("No se encontraron las columnas esperadas para la leyenda detalle.")

    # Vista resumida adicional solicitada (según agrupación)
    if agrupar_por == "Promotor":
        print("\n=== Vista resumida para leyenda detalle (Promotor) ===")

        if df_agrupado.empty:
            print("df_agrupado está vacío, no hay datos para mostrar.")
            return

        # Creamos una copia para no alterar el DF original
        df_vista = df_agrupado.copy()

        # Nueva columna: clientes / área_m2 (controlando división por cero o área nula)
        df_vista["clientes_por_area_m2"] = df_vista.apply(
            lambda r: r["clientes_total"] / r["area_m2"]
            if (r.get("area_m2") not in [None, 0] and pd.notna(r.get("area_m2")))
            else None,
            axis=1,
        )

        # Renombrar columnas a los nombres que quieres ver
        df_vista = df_vista.rename(
            columns={
                "apellido_promotor": "Promotor",
                "muestras_total": "#Muestras",
                "clientes_total": "#Clientes",
            }
        )

        # Definir el orden y subconjunto de columnas que quieres ver
        columnas_vista = [
            "Promotor",
            "#Muestras",
            "#Clientes",
            "area_m2",
            "clientes_por_area_m2",
            "dias_habiles",
            "pct_clientes_no_fieles",
            "pct_total_muestras_contactables",
            "pct_contactabilidad_no_fieles",
        ]

        # Nos quedamos solo con las columnas que existan realmente
        columnas_vista_presentes = [c for c in columnas_vista if c in df_vista.columns]
        # Ordenar por #Clientes (desc) si está disponible
        if "#Clientes" in df_vista.columns:
            df_vista = df_vista.sort_values("#Clientes", ascending=False)
        elif "clientes_total" in df_vista.columns:
            df_vista = df_vista.sort_values("clientes_total", ascending=False)

        print(df_vista[columnas_vista_presentes].head(50).to_string(index=False))
    else:
        print("\n=== Vista resumida para leyenda detalle (Mes) ===")

        if df_agrupado.empty:
            print("df_agrupado está vacío, no hay datos para mostrar.")
            return

        df_vista = df_agrupado.copy()
        # Calcular clientes / área
        df_vista["clientes_por_area_m2"] = df_vista.apply(
            lambda r: r["clientes_total"] / r["area_m2"]
            if (r.get("area_m2") not in [None, 0] and pd.notna(r.get("area_m2")))
            else None,
            axis=1,
        )
        # Renombres para encabezados
        df_vista = df_vista.rename(
            columns={
                "mes": "Mes",
                "muestras_total": "#Muestras",
                "clientes_total": "#Clientes",
            }
        )
        columnas_vista_mes = [
            "Mes",
            "#Muestras",
            "#Clientes",
            "area_m2",
            "clientes_por_area_m2",
            "dias_habiles",
            "pct_clientes_no_fieles",
            "pct_total_muestras_contactables",
            "pct_contactabilidad_no_fieles",
        ]
        columnas_vista_presentes = [c for c in columnas_vista_mes if c in df_vista.columns]
        # Orden por #Clientes desc
        if "#Clientes" in df_vista.columns:
            df_vista = df_vista.sort_values("#Clientes", ascending=False)
        elif "clientes_total" in df_vista.columns:
            df_vista = df_vista.sort_values("clientes_total", ascending=False)
        print(df_vista[columnas_vista_presentes].head(50).to_string(index=False))


if __name__ == "__main__":
    main()
