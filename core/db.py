import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConnection, cursor as PGCursor
from core.config import get_settings, get_log_dir
import time
from functools import wraps # For decorators
from typing import Callable, TypeVar, Any # For type hinting
from contextlib import contextmanager
import logging
from logging.handlers import RotatingFileHandler







def connect_db() -> PGConnection:
    """
    Cria e retorna uma nova conexão ao PostgreSQL utilizando a string de conexão
    fornecida em SUPABASE_DB_URL. Usa RealDictCursor para retornar dicts.
    """
    settings = get_settings()
    if not settings.supabase_db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL não configurada. Informe a connection string no .env."
        )
    conn: PGConnection = psycopg2.connect(
        settings.supabase_db_url,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    return conn



def execute_write(conn: PGConnection, sql: str, params: tuple | dict = ()) -> int:
    """
    Execute a write operation (INSERT, UPDATE, DELETE). Returns affected row count.
    """
    cur: PGCursor = conn.cursor()
    try:
        cur.execute(sql, params)
        cur.execute(sql, params)
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()

@contextmanager
def transaction(conn: PGConnection):
    try:
        conn.autocommit = False
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise

def execute_query(conn: PGConnection, sql: str, params: tuple | dict = ()) -> list[dict]:
    cur: PGCursor = conn.cursor()
    try:
        cur.execute(sql, params)
        # RealDictCursor já retorna dicts
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()

def fetch_all(conn: PGConnection, sql: str, params: tuple | dict = ()) -> list[dict]:
    return execute_query(conn, sql, params)

def fetch_one(conn, sql: str, params: tuple | dict = ()) -> dict | None:
    """
    Execute a query and return a single row as a dict, or None if no rows.
    """
    rows = execute_query(conn, sql, params)
    return rows[0] if rows else None


#============================

# -----------------------------------------------------------------------------
# Schema versioning for PostgreSQL
# -----------------------------------------------------------------------------
SCHEMA_VERSION: int = 1  # bump this when you add a new migration

def _ensure_schema_version_table(conn: "PGConnection") -> None:
    """
    Ensures the schema_version table exists.
    This replaces SQLite's PRAGMA user_version.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL
            );
        """)
        # Initialize to 0 if empty
        cur.execute("SELECT COUNT(*) AS n FROM schema_version;")
        row = cur.fetchone()
        if row and (row.get("n") == 0 or row.get("count") == 0):
            cur.execute("INSERT INTO schema_version (version) VALUES (0);")
    conn.commit()

def _get_schema_version(conn: "PGConnection") -> int:
    """
    Returns the current schema version (0 if not set).
    """
    _ensure_schema_version_table(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM schema_version LIMIT 1;")
        row = cur.fetchone()
        return int(row["version"]) if row and "version" in row else 0

def _set_schema_version(conn: "PGConnection", v: int) -> None:
    """
    Sets the schema version to v.
    """
    with conn.cursor() as cur:
        # Update the single row; if no row exists, insert it.
        cur.execute("UPDATE schema_version SET version = %s;", (v,))
        if cur.rowcount == 0:
            cur.execute("INSERT INTO schema_version (version) VALUES (%s);", (v,))
    conn.commit()

# -----------------------------------------------------------------------------
# Migrations registry
# -----------------------------------------------------------------------------
# Each migration function must transform from (version-1) to 'version'.
# Example: MIGRATIONS[1] creates the baseline schema.
from typing import Callable, Dict

MIGRATIONS: Dict[int, Callable[["PGConnection"], None]] = {}

def _migration_1_create_baseline(conn: "PGConnection") -> None:
    """
    Baseline schema for PostgreSQL.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id          SERIAL PRIMARY KEY,
                dt          DATE NOT NULL,
                category    TEXT NOT NULL,
                subcategory TEXT,
                amount      NUMERIC NOT NULL,
                note        TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_expenses_dt ON expenses(dt);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_expenses_category ON expenses(category);")
    conn.commit()

MIGRATIONS[1] = _migration_1_create_baseline

def _migrate(conn: "PGConnection", target_version: int) -> None:
    """
    Applies incremental migrations up to 'target_version'.
    Each step runs in a transaction: success -> set schema_version; failure -> rollback.
    """
    logger = _get_logger()
    current = _get_schema_version(conn)

    if current > target_version:
        raise RuntimeError(
            f"Database schema version {current} is newer than expected {target_version}. "
            "Update the application or handle downgrades explicitly."
        )
    if current == target_version:
        logger.info("Schema is up-to-date (v%s).", current)
        return

    for nxt in range(current + 1, target_version + 1):
        mig = MIGRATIONS.get(nxt)
        if mig is None:
            raise RuntimeError(f"No migration registered for version {nxt}.")
        logger.info("Applying migration %s → %s ...", nxt - 1, nxt)
        try:
            # Start transaction explicitly
            conn.autocommit = False
            mig(conn)
            _set_schema_version(conn, nxt)
            conn.commit()
            logger.info("Migration to v%s completed.", nxt)
        except Exception as exc:
            conn.rollback()
            logger.error("Migration to v%s failed: %s", nxt, exc)
            raise
        finally:
            conn.autocommit = True

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def ensure_schema() -> None:
    """
    Ensures the database schema is at the expected version (SCHEMA_VERSION).
    Opens a connection and applies any missing migrations.
    """
    logger = _get_logger()
    with connect_db() as conn:
        logger.info("Checking schema version...")
        _migrate(conn, SCHEMA_VERSION)
        logger.info("Schema ensured at version %s.", SCHEMA_VERSION)

def ensure_db_ready(touch: bool = True) -> None:
    """
    Ensures the database is reachable and writable.
    - Executes a simple SELECT 1;
    - Optionally performs a lightweight write (create/drop a temp table).
    """
    logger = _get_logger()
    with connect_db() as conn:
        # Reachability check
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            _ = cur.fetchone()

        if touch:
            # Lightweight write check
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE __ping__ (id INTEGER);")
                cur.execute("DROP TABLE __ping__;")
            conn.commit()

    logger.info("Database is ready (reachability + write checked).")

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
_LOGGER: logging.Logger | None = None

def _get_logger() -> logging.Logger:
    """
    Returns a module-level logger configured with a rotating file handler.
    The logger is created once (singleton) and reused across calls.
    """
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    log_dir: Path = get_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("core.db")  # keep a stable name for filters
    logger.setLevel(logging.INFO)
    logger.propagate = False  # avoid duplicate logs in root logger

    log_file = log_dir / "app.log"
    handler = RotatingFileHandler(
        log_file,
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="YYYY-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    # Add handler only once
    if not logger.handlers:
        logger.addHandler(handler)

    _LOGGER = logger
    return logger






