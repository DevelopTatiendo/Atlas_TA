# db_utils.py
import os
from functools import lru_cache
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import pandas as pd

@lru_cache(maxsize=4)
def get_engine(schema: str | None = "fullclean_telemercadeo"):
    user = os.getenv("DB_USER")
    pwd = quote_plus(os.getenv("DB_PASSWORD") or "")
    host = os.getenv("DB_HOST")
    # Usa mysql+mysqlconnector con SQLAlchemy 2.x
    url = f"mysql+mysqlconnector://{user}:{pwd}@{host}/{schema}?charset=utf8mb4"
    return create_engine(url, future=True, pool_pre_ping=True)

def sql_read(query: str, params: list | dict | None = None, schema: str | None = "fullclean_telemercadeo") -> pd.DataFrame:
    engine = get_engine(schema)
    # SQLAlchemy 2.x: usar Connection + text(query)
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)
