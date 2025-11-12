import psycopg2
import psycopg2.extras
from core.config import get_settings, get_log_dir
import time
from functools import wraps # For decorators
from typing import Callable, TypeVar, Any # For type hinting
from contextlib import contextmanager
import logging
from logging.handlers import RotatingFileHandler







def connect_db():
    """
    Cria e retorna uma nova conexão ao PostgreSQL utilizando a string de conexão
    fornecida em SUPABASE_DB_URL. Usa RealDictCursor para retornar dicts.
    """
    settings = get_settings()
    if not settings.supabase_db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL não configurada. Informe a connection string no .env."
        )
    conn = psycopg2.connect(
        settings.supabase_db_url,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    return conn



def execute_write(conn, sql: str, params: tuple | dict = ()) -> int:
    """
    Executa uma operação de escrita (INSERT, UPDATE, DELETE) no PostgreSQL.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()
        return cur.rowcountv

@contextmanager
def transaction(conn):
    try:
        with conn:
            yield
    except Exception:
        conn.rollback()
        raise


#============================



def _migration_1_create_baseline(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id SERIAL PRIMARY KEY,
                dt DATE NOT NULL,
                category TEXT NOT NULL,
                subcategory TEXT,
                amount NUMERIC NOT NULL,
                note TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_expenses_dt ON expenses(dt);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_expenses_category ON expenses(category);")
        conn.commit()








