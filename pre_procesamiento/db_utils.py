# db_utils.py
import os
from functools import lru_cache
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd


def _get_driver() -> tuple[str, dict]:
    """Elige el mejor driver MySQL disponible.

    Preferencia: PyMySQL (pure Python, sin C extension, estable en Windows)
    Fallback:    mysql-connector-python (versión >=9.x puede causar segfault
                 en entornos threaded como Streamlit — instalar PyMySQL si ocurre)

    Retorna (dialect_string, connect_args).
    """
    try:
        import pymysql  # noqa: F401
        return "mysql+pymysql", {"connect_timeout": 10}
    except ImportError:
        pass
    # Fallback: pip install PyMySQL si la app crashea en consultar_db
    return "mysql+mysqlconnector", {"connection_timeout": 10}


@lru_cache(maxsize=4)
def get_engine(schema: str | None = "fullclean_telemercadeo"):
    user = os.getenv("DB_USER")
    pwd = quote_plus(os.getenv("DB_PASSWORD") or "")
    host = os.getenv("DB_HOST")
    dialect, connect_args = _get_driver()
    url = f"{dialect}://{user}:{pwd}@{host}/{schema}?charset=utf8mb4"
    print(f"  [db] driver={dialect}  schema={schema}", flush=True)
    return create_engine(
        url,
        future=True,
        # NullPool: una conexion nueva por query, sin pool compartido.
        # Evita deadlocks entre threads de Streamlit y el pool de SQLAlchemy.
        # mysql-connector-python >=9.x tiene bugs con QueuePool en threads.
        poolclass=NullPool,
        connect_args=connect_args,
    )


def sql_read(query: str, params: list | dict | None = None, schema: str | None = "fullclean_telemercadeo") -> pd.DataFrame:
    engine = get_engine(schema)
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)
