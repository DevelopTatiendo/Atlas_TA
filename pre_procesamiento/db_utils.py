# db_utils.py
import os
from functools import lru_cache
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd


def _get_driver() -> tuple:
    """PyMySQL (pure Python) primero; mysql-connector como fallback.
    mysql-connector-python 9.x causa segfault silencioso con SQLAlchemy
    en contexto threaded de Streamlit. Instalar: pip install PyMySQL
    """
    try:
        import pymysql  # noqa: F401
        return "mysql+pymysql", {"connect_timeout": 10}
    except ImportError:
        pass
    return "mysql+mysqlconnector", {"connection_timeout": 10}


@lru_cache(maxsize=4)
def get_engine(schema=None):
    if schema is None:
        schema = "fullclean_telemercadeo"
    user = os.getenv("DB_USER")
    pwd  = quote_plus(os.getenv("DB_PASSWORD") or "")
    host = os.getenv("DB_HOST")
    dialect, connect_args = _get_driver()
    url = f"{dialect}://{user}:{pwd}@{host}/{schema}?charset=utf8mb4"
    print(f"  [db] driver={dialect}  schema={schema}", flush=True)
    return create_engine(
        url,
        future=True,
        poolclass=NullPool,       # sin pool compartido: evita conflictos en threads de Streamlit
        connect_args=connect_args,
    )


def sql_read(query: str, params=None, schema=None) -> pd.DataFrame:
    if schema is None:
        schema = "fullclean_telemercadeo"
    engine = get_engine(schema)
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)
