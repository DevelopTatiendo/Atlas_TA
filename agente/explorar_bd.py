"""explorar_bd.py — Exploración autónoma de la BD por el agente.

El agente puede llamar estas funciones para entender tablas,
columnas y relaciones ANTES de construir una consulta analítica.

REGLA ABSOLUTA: Solo SELECT y SHOW. Nunca INSERT, UPDATE, DELETE, DROP.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pre_procesamiento.db_utils import sql_read


# ── Utilidades internas ───────────────────────────────────────────────────────

_BASES_PERMITIDAS = {
    "fullclean_contactos",
    "fullclean_telemercadeo",
    "fullclean_general",
    "fullclean_personal",
    "fullclean_cartera",
    "fullclean_quejas",
}


def _safe_schema(schema: str) -> str:
    if schema not in _BASES_PERMITIDAS:
        raise ValueError(f"Base '{schema}' no permitida. Usar: {_BASES_PERMITIDAS}")
    return schema


# ── EXPLORACIÓN DE ESTRUCTURA ─────────────────────────────────────────────────

def listar_tablas(schema: str = "fullclean_contactos") -> list[str]:
    """Lista todas las tablas y vistas de una base de datos."""
    _safe_schema(schema)
    df = sql_read(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = :s ORDER BY table_type, table_name",
        params={"s": schema},
        schema=schema,
    )
    return df.to_dict(orient="records")


def describir_tabla(tabla: str, schema: str = "fullclean_contactos") -> list[dict]:
    """Devuelve columnas, tipos y llaves de una tabla."""
    _safe_schema(schema)
    df = sql_read(
        "SELECT column_name, column_type, is_nullable, column_key, column_comment "
        "FROM information_schema.columns "
        "WHERE table_schema = :s AND table_name = :t "
        "ORDER BY ordinal_position",
        params={"s": schema, "t": tabla},
        schema=schema,
    )
    return df.to_dict(orient="records")


def muestra_tabla(tabla: str, schema: str = "fullclean_contactos",
                  limite: int = 5, filtro_sql: str = "") -> list[dict]:
    """Retorna N filas de muestra de una tabla (sin datos sensibles de BD).

    Args:
        tabla:      Nombre de la tabla.
        schema:     Base de datos.
        limite:     Número de filas (máx 20).
        filtro_sql: Cláusula WHERE adicional (ej: "id_ciudad = 3").
                    Solo se acepta si no contiene palabras peligrosas.
    """
    _safe_schema(schema)
    limite = min(int(limite), 20)

    # Seguridad mínima: rechazar palabras peligrosas en el filtro
    palabras_peligrosas = ["insert", "update", "delete", "drop", "alter",
                           "create", "truncate", "exec", "call", "--", ";"]
    filtro_lower = filtro_sql.lower()
    for p in palabras_peligrosas:
        if p in filtro_lower:
            raise ValueError(f"Filtro rechazado: contiene '{p}'")

    where = f"WHERE {filtro_sql}" if filtro_sql.strip() else ""
    df = sql_read(
        f"SELECT * FROM `{tabla}` {where} LIMIT {limite}",
        schema=schema,
    )

    # Convertir timestamps a string para JSON
    for col in df.select_dtypes(include=["datetime64", "object"]).columns:
        df[col] = df[col].astype(str)

    return df.to_dict(orient="records")


def contar_registros(tabla: str, schema: str = "fullclean_contactos",
                     filtro_sql: str = "") -> dict:
    """Cuenta registros en una tabla con filtro opcional."""
    _safe_schema(schema)

    palabras_peligrosas = ["insert", "update", "delete", "drop", "--", ";"]
    if any(p in filtro_sql.lower() for p in palabras_peligrosas):
        raise ValueError("Filtro rechazado por seguridad")

    where = f"WHERE {filtro_sql}" if filtro_sql.strip() else ""
    df = sql_read(f"SELECT COUNT(*) AS n FROM `{tabla}` {where}", schema=schema)
    return {"tabla": tabla, "schema": schema, "n_registros": int(df["n"].iloc[0])}


def explorar_relacion(
    tabla_origen: str, columna: str,
    schema_origen: str = "fullclean_contactos",
    limite: int = 10,
) -> dict:
    """Muestra valores únicos de una columna para entender su dominio.

    Útil para explorar columnas de FK o campos categóricos antes de un JOIN.
    """
    _safe_schema(schema_origen)
    df = sql_read(
        f"SELECT `{columna}`, COUNT(*) AS frecuencia "
        f"FROM `{tabla_origen}` "
        f"WHERE `{columna}` IS NOT NULL "
        f"GROUP BY `{columna}` "
        f"ORDER BY frecuencia DESC "
        f"LIMIT {min(limite, 50)}",
        schema=schema_origen,
    )
    return {
        "tabla": tabla_origen,
        "columna": columna,
        "valores_top": df.to_dict(orient="records"),
    }


def ejecutar_select(sql: str, schema: str = "fullclean_contactos",
                    limite: int = 200) -> list[dict]:
    """Ejecuta una consulta SELECT arbitraria.

    El agente puede usar esto para consultas personalizadas una vez que
    conoce la estructura de las tablas.

    Restricciones:
    - Solo permite SELECT y WITH (CTEs).
    - Límite automático de 200 filas si no hay LIMIT en la query.
    - Rechaza cualquier instrucción de escritura.
    """
    _safe_schema(schema)

    sql_stripped = sql.strip().lower()
    if not (sql_stripped.startswith("select") or sql_stripped.startswith("with")):
        raise ValueError("Solo se permiten consultas SELECT o WITH (CTEs).")

    palabras_prohibidas = ["insert", "update", "delete", "drop", "alter",
                           "create", "truncate", "exec", "call"]
    for p in palabras_prohibidas:
        if f" {p} " in f" {sql_stripped} ":
            raise ValueError(f"Consulta rechazada: contiene '{p}'")

    # Agregar LIMIT si no existe
    if "limit" not in sql_stripped:
        sql = sql.rstrip().rstrip(";") + f" LIMIT {limite}"

    df = sql_read(sql, schema=schema)

    for col in df.columns:
        if df[col].dtype == "object" or str(df[col].dtype).startswith("datetime"):
            df[col] = df[col].astype(str)

    return df.to_dict(orient="records")


# ── DIAGNÓSTICOS PREDEFINIDOS ─────────────────────────────────────────────────

def diagnostico_rutas(id_ciudad: int = 3) -> dict:
    """Diagnóstico completo del modelo de rutas para una ciudad.

    Verifica: existencia de rutas, barrios asignados, clientes por ruta,
    si hay coordenadas en eventos de esa ciudad.
    """
    # 1. Rutas disponibles
    # rutas_cobro.ruta es el nombre (no .nombre)
    rutas = ejecutar_select(
        f"""
        SELECT rc.id, rc.ruta, COUNT(rcz.id_barrio) AS n_barrios
        FROM fullclean_contactos.rutas_cobro rc
        LEFT JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_ruta_cobro = rc.id
        GROUP BY rc.id, rc.ruta
        ORDER BY n_barrios DESC
        """,
        schema="fullclean_contactos",
    )

    # 2. Clientes por barrio con ruta asignada en esa ciudad
    # barrios.barrio es el nombre (no .nombre); contactos.id es la PK
    clientes_en_rutas = ejecutar_select(
        f"""
        SELECT
            rc.ruta,
            b.barrio,
            COUNT(c.id) AS n_clientes
        FROM fullclean_contactos.contactos c
        JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
        JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_barrio = b.Id
        JOIN fullclean_contactos.rutas_cobro rc ON rc.id = rcz.id_ruta_cobro
        WHERE b.id_ciudad = {id_ciudad}
        GROUP BY rc.ruta, b.barrio
        ORDER BY rc.ruta, n_clientes DESC
        """,
        schema="fullclean_contactos",
    )

    # 3. ¿Hay coordenadas en eventos de esta ciudad?
    # coordenada_latitud/longitud son VARCHAR en vwEventos (no latitud/longitud)
    # id_centroope no está en vwEventos — filtrar via JOIN ciudades
    coords = ejecutar_select(
        f"""
        SELECT
            COUNT(*) AS total_eventos,
            SUM(CASE WHEN e.coordenada_latitud IS NOT NULL
                      AND e.coordenada_latitud != ''
                      AND e.coordenada_latitud != '0' THEN 1 ELSE 0 END) AS con_coordenada,
            ROUND(100 * SUM(CASE WHEN e.coordenada_latitud IS NOT NULL
                                  AND e.coordenada_latitud != ''
                                  AND e.coordenada_latitud != '0' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_con_coord
        FROM fullclean_contactos.vwEventos e
        INNER JOIN fullclean_contactos.vwContactos c ON c.id = e.id_contacto
        INNER JOIN fullclean_contactos.ciudades ciu ON ciu.id = c.id_ciudad
        WHERE ciu.id_centroope = {id_ciudad}
        """,
        schema="fullclean_contactos",
    )

    # 4. ¿Pedidos tienen id_contacto?
    pedidos_check = ejecutar_select(
        """
        SELECT
            COUNT(*) AS total_pedidos,
            SUM(CASE WHEN id_contacto IS NOT NULL AND id_contacto > 0 THEN 1 ELSE 0 END) AS con_contacto,
            SUM(CASE WHEN id_vendedor IS NOT NULL AND id_vendedor > 0 THEN 1 ELSE 0 END) AS con_vendedor
        FROM fullclean_telemercadeo.pedidos
        WHERE fecha_hora_pedido >= '2026-01-01'
        """,
        schema="fullclean_telemercadeo",
    )

    return {
        "ciudad_id": id_ciudad,
        "rutas_disponibles": rutas,
        "clientes_en_rutas_muestra": clientes_en_rutas[:20],
        "cobertura_coordenadas": coords,
        "pedidos_linkage": pedidos_check,
    }


def diagnostico_quejas_zona(id_ciudad: int = 3) -> dict:
    """Verifica disponibilidad de quejas geolocalizadas por barrio."""
    # barrios.barrio es el nombre; contactos.id es la PK
    resultado = ejecutar_select(
        f"""
        SELECT
            b.barrio,
            COUNT(q.id) AS n_quejas
        FROM fullclean_quejas.quejas q
        JOIN fullclean_contactos.contactos c ON c.id = q.id_contacto
        JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
        WHERE b.id_ciudad = {id_ciudad}
        GROUP BY b.barrio
        ORDER BY n_quejas DESC
        """,
        schema="fullclean_quejas",
    )
    return {"ciudad_id": id_ciudad, "quejas_por_barrio": resultado}


# ── CLI de diagnóstico ────────────────────────────────────────────────────────

if __name__ == "__main__":
    from config.secrets_manager import load_env_secure
    load_env_secure(prefer_plain=True, enc_path="config/.env.enc",
                    pass_env_var="MAPAS_SECRET_PASSPHRASE", cache=False)

    import argparse
    parser = argparse.ArgumentParser(description="Diagnóstico de BD para Atlas Agent")
    parser.add_argument("--ciudad", type=int, default=3, help="ID de ciudad (default: 3 = Medellín)")
    parser.add_argument("--tabla", type=str, help="Describir una tabla específica")
    parser.add_argument("--schema", type=str, default="fullclean_contactos")
    args = parser.parse_args()

    if args.tabla:
        print(f"\n── Columnas de {args.schema}.{args.tabla} ──")
        cols = describir_tabla(args.tabla, args.schema)
        for c in cols:
            print(f"  {c['column_name']:35} {c['column_type']:25} {c.get('column_comment','')}")
    else:
        print(f"\n── Diagnóstico de rutas para ciudad {args.ciudad} ──")
        resultado = diagnostico_rutas(args.ciudad)
        print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
