"""
diagnostico_crash.py  —  Atlas TA
Ejecutar en PowerShell: python diagnostico_crash.py
Verifica compatibilidad de paquetes y reproduce el flujo de consultar_db
SIN Streamlit para aislar si el crash es de pandas/numpy o de Streamlit.
"""
import sys, os

print("=" * 60)
print("DIAGNÓSTICO ATLAS TA - CRASH ATLAS TIMING")
print("=" * 60)

# 1. Versiones críticas
print("\n[1] VERSIONES DE PAQUETES")
pkgs = ["numpy", "pandas", "streamlit", "sqlalchemy", "mysql.connector"]
for p in pkgs:
    try:
        mod = __import__(p.replace(".", "_") if "." in p else p)
        ver = getattr(mod, "__version__", "?")
        print(f"  {p:<30} {ver}")
    except ImportError as e:
        print(f"  {p:<30} NO INSTALADO ({e})")

# 2. Compatibilidad numpy/pandas
print("\n[2] COMPATIBILIDAD numpy ↔ pandas")
try:
    import numpy as np
    import pandas as pd
    major_np = int(np.__version__.split(".")[0])
    major_pd = int(pd.__version__.split(".")[0])
    if major_np >= 2 and major_pd < 2:
        print(f"  ⚠️  INCOMPATIBILIDAD DETECTADA:")
        print(f"      numpy {np.__version__} (ABI 2.x) + pandas {pd.__version__} (compilado para numpy 1.x)")
        print(f"      Esto causa segfault silencioso en pd.read_sql() y otras ops vectoriales.")
        print(f"\n  SOLUCIÓN: pip install numpy==1.25.0 --force-reinstall")
        print(f"  O bien:   pip install 'pandas>=2.1.0'")
    else:
        print(f"  ✅ numpy {np.__version__} + pandas {pd.__version__} — OK")
        # Test básico
        df = pd.DataFrame({"a": np.array([1.0, 2.0, 3.0])})
        result = df["a"].mean()
        print(f"  ✅ Operación pandas/numpy básica OK (mean={result})")
except Exception as e:
    print(f"  ❌ Error: {e}")

# 3. Importar módulos del proyecto
print("\n[3] IMPORTS DEL PROYECTO")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from config.secrets_manager import load_env_secure
    load_env_secure()
    print("  ✅ secrets_manager OK")
except Exception as e:
    print(f"  ❌ secrets_manager: {e}")
    sys.exit(1)

try:
    from pre_procesamiento.db_utils import sql_read
    print("  ✅ db_utils OK")
except Exception as e:
    print(f"  ❌ db_utils: {e}")

try:
    from pre_procesamiento.preprocesamiento_muestras import consultar_db, crear_df
    print("  ✅ preprocesamiento_muestras OK (sin @st.cache_data activo)")
except Exception as e:
    print(f"  ❌ preprocesamiento_muestras: {e}")

# 4. Test de conexión BD
print("\n[4] TEST CONEXIÓN BD")
try:
    # Verificar driver mysql-connector
    try:
        import mysql.connector as mc
        print(f"  mysql-connector-python: {mc.__version__}")
        # Verificar si C extension o pure Python
        try:
            import _mysql_connector
            print("  ⚠️  C extension (_mysql_connector) disponible — puede causar crash en modo threaded")
        except ImportError:
            print("  ✅ C extension no disponible — se usa pure Python (seguro)")
    except ImportError:
        print("  ⚠️  mysql-connector-python no importable")

    from pre_procesamiento.db_utils import sql_read
    df_ping = sql_read("SELECT 1 AS ok", schema="fullclean_contactos")
    print(f"  ✅ fullclean_contactos alcanzable: {df_ping.to_dict()}")
except Exception as e:
    import traceback
    print(f"  ❌ BD error: {e}")
    traceback.print_exc()
    sys.exit(1)

# 5. Reproducir consultar_db SIN @st.cache_data
print("\n[5] CONSULTAR_DB DIRECTO (sin cache Streamlit)")
print("    Ciudad: Cali (centroope=2), rango: 30 días recientes")
try:
    import pandas as pd
    from pre_procesamiento.db_utils import sql_read
    from sqlalchemy import text

    query = """
        SELECT
            e.idEvento        AS id_muestra,
            e.id_contacto     AS id_contacto,
            e.fecha_evento    AS fecha_evento,
            e.id_evento_tipo  AS id_evento_tipo,
            e.id_autor        AS id_promotor,
            e.coordenada_longitud,
            e.coordenada_latitud,
            per.apellido      AS apellido_promotor,
            MONTH(e.fecha_evento) AS mes
        FROM fullclean_contactos.vwEventos e
        INNER JOIN fullclean_contactos.vwContactos con ON con.id = e.id_contacto
        INNER JOIN fullclean_contactos.ciudades ciu ON ciu.id = con.id_ciudad
        INNER JOIN fullclean_personal.personal per ON per.id = e.id_autor AND per.id_cargo = 39
        WHERE
            e.fecha_evento BETWEEN :fecha_inicio AND :fecha_fin
            AND ciu.id_centroope = :id_centroope
            AND e.id_evento_tipo = 15
            AND e.coordenada_longitud <> 0
            AND e.coordenada_latitud <> 0
        LIMIT 500
    """
    params = {
        "fecha_inicio": "2025-01-01 00:00:00",
        "fecha_fin":    "2025-01-31 23:59:59",
        "id_centroope": 2,
    }
    import time
    t0 = time.perf_counter()
    df = sql_read(query, params=params, schema="fullclean_contactos")
    elapsed = time.perf_counter() - t0
    print(f"  ✅ Resultado: {len(df)} filas en {elapsed:.2f}s")
    print(f"     Columnas: {list(df.columns)}")
    print(f"     dtypes:\n{df.dtypes.to_string()}")
except Exception as e:
    import traceback
    print(f"  ❌ Error en consulta: {e}")
    traceback.print_exc()

# 6. Test pickle (lo que hace @st.cache_data internamente)
print("\n[6] TEST PICKLE DEL DATAFRAME (simula @st.cache_data)")
try:
    import pickle, io
    buf = io.BytesIO()
    pickle.dump(df, buf)
    buf.seek(0)
    df2 = pickle.load(buf)
    print(f"  ✅ pickle/unpickle OK — {len(df2)} filas recuperadas")
except Exception as e:
    print(f"  ❌ Pickle FALLA: {e} — ESTO ES EL CRASH")

print("\n" + "=" * 60)
print("DIAGNÓSTICO COMPLETO")
print("=" * 60)
