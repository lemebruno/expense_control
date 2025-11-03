import sqlite3
from pathlib import Path
from core.config import (
    get_db_path,
    get_settings,
    get_log_dir
)
import time
from functools import wraps # For decorators
from typing import Callable, TypeVar, Any # For type hinting
from contextlib import contextmanager
import logging
from logging.handlers import RotatingFileHandler







# Apply SQLite pragmas to the given connection
def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """
        Applies SQLite PRAGMA settings to the given database connection based on application settings.
    """
    s = get_settings()
    conn.execute(f"PRAGMA busy_timeout = {int(s.db_busy_timeout_ms)};")
    conn.execute(f"PRAGMA journal_mode = {s.db_journal_mode};")
    conn.execute(f"PRAGMA synchronous = {s.db_synchronous};")
    conn.execute("PRAGMA foreign_keys = ON;") #Enable foreign key constraints
    conn.execute("PRAGMA temp_store = MEMORY;") #Use memory for temp storage


#============================
# Database connection
def connect_db() -> sqlite3.Connection:
    """
        Creates and returns a new SQLite database connection with configured pragmas.
    """
    lg = _get_logger()
    db_path = get_db_path()
    lg.info("Opening database connection to %s", db_path)
    conn = sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES,
        )
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn

#============================
# SQLite error handling
def is_locked_error(exc: Exception) -> bool:
    """
        Checks if the given exception is a SQLite "database is locked" error.
    """
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    msg = str(exc).lower()
    return "database is locked" in msg or "database table is locked" in msg

#============================
F = TypeVar("F", bound=Callable[..., Any]) #Generic type for functions
def retry_on_locked(attempts:int = 3,delay: float = 0.15, backoff:float=2.0) -> Callable[[F],F]:
    """
        Decorator that retries a function if it raises a SQLite "database is locked" error.
        
        Parameters:
            attempts (int): Number of retry attempts.
            delay (float): Initial delay between retries in seconds.
            backoff (float): Multiplier to increase delay after each failed attempt.
        Use:
            @retry_on_locked(attempts=5, delay=0.2, backoff=2.0)
            def my_db_function(...):
                ...
        Observations:
            - Only retries on "database is locked" errors.
            - Other exceptions are raised immediately.
    """
    def decorator(func: F) -> F:
        @wraps(func)
        
        def wrapper(*args:Any,**kwargs:Any)-> Any:
            lg = _get_logger()
            tries = max(1,int(attempts))
            wait = float(delay)
            last_exc:Exception | None = None #Store last locked exception

            for i in range(tries):
                try:
                    return func(*args,**kwargs)
                except Exception as exc:
                    if  is_locked_error(exc) and i <tries -1:
                            time.sleep(wait)
                            lg.warning("database is locked → retry %s (%d/%d) wait=%.2fs",
                                       func.__name__,i+2,tries,wait)
                            time.sleep(wait)
                            wait *=backoff                                                      
                            continue
                    raise
            #If we reach here, all attempts failed due to locked error
            lg.error("Write failed in %s: %s", func.__name__, exc)
            raise
        return wrapper  
    return decorator

@retry_on_locked(attempts=4,delay=0.2,backoff=1.8)
def execute_write(conn: sqlite3.Connection, sql: str, params: tuple |dict =())-> int:
    """
        Executes a write operation (INSERT, UPDATE, DELETE) on the database with retry on locked errors.
        
        Parameters:
            conn (sqlite3.Connection): The database connection.
            sql (str): The SQL statement to execute.
            params (tuple | dict): The parameters for the SQL statement.
        
        Returns:
            int: The number of rows affected by the operation.
    """
   
    lg = _get_logger()
    lg.debug("Executing write: %s | params=%s", sql, params)
    cur= conn.execute(sql,params)
    conn.commit()
    lg.info("Write committed; rowcount=%s", cur.rowcount)
    return cur.rowcount

@contextmanager
def transaction(conn:sqlite3.Connection,begin_mode:str = "DEFERRED"):
    """
        Context manager for a database transaction with specified begin mode.
        
        Parameters:
            conn (sqlite3.Connection): The database connection.
            begin_mode (str): The transaction begin mode ("DEFERRED", "IMMEDIATE", "EXCLUSIVE").
        
        Usage:
            with transaction(conn, begin_mode="IMMEDIATE"):
                # Perform database operations here
    """    
    mode = begin_mode.strip().upper()
    if mode not in {"DEFERRED","IMMEDIATE","EXCLUSIVE"}:
        raise ValueError(f"Invalid transaction begin mode: {begin_mode}")
    try:
        conn.execute(f"BEGIN {mode};")
        yield
        conn.commit()
    except:
        conn.rollback()
        raise

def ensure_db_ready(touch:bool = True) ->None:
    """
        Ensures that the database is ready for operations by executing a simple query.
        Optionally performs a write operation to verify write access.
    """
    lg = _get_logger()
    db_path = get_db_path()
    try:
        with connect_db() as conn:
            conn.execute("PRAGMA user_version;")
            if touch:
                conn.execute("CREATE TABLE IF NOT EXISTS __ping__ (id INTEGER);")
                conn.execute("DROP TABLE IF EXISTS __ping__;")    
        lg.info("DB ready at %s",db_path)
    except sqlite3.Error as e:
        lg.error("SQLite ping/creation failed at %s: %s",db_path, e)
        raise RuntimeError(f"SQLite ping / creation failed at {db_path}:{e}") from e
#============================


def row_to_dict(row: sqlite3.Row)-> dict[str,Any]:
    """
        Converts an sqlite3.Row object into a dictionary for easy serialization/use in APIs.
    """
    return {k:row[k] for k in row.keys()}

def rows_to_dicts(rows:list [sqlite3.Row])->list[dict[str,Any]]:
    """
        Converts a list of sqlite3.Row objects into a list of dictionaries.
    """
    return [row_to_dict(r) for r in rows]

def execute_query(conn: sqlite3.Connection, sql:str, params:tuple | dict = ()) -> list[sqlite3.Row]:
    """
        Executes a parameterized SELECT query and returns a list of sqlite3.Row objects (without committing).
        Usage: rows = execute_query(conn, "SELECT * FROM expenses WHERE dt >= ?", (start_dt,))
    """

    lg = _get_logger()
    lg.debug("Executing query: %s | params =%s", sql, params)
    cur = conn.execute(sql,params)
    rows = cur.fetchall()
    lg.debug("Query returned %d row(s)",len(rows))
    return rows

def fetch_one(conn: sqlite3.Connection,sql:str,params:tuple | dict = ()) -> sqlite3.Row |None:
    """
        Returns a single row (or None) for SELECT statements that expect a single result.
    """
    rows = execute_query(conn,sql,params)
    return rows[0] if rows else None

def fetch_all(conn: sqlite3.Connection, sql: str, params: tuple | dict = ()) -> list[sqlite3.Row]:
   """
    Returns ALL rows (shortcut for execute_query).
   """
   return execute_query(conn, sql, params)

def query(sql:str,params:tuple | dict = ())-> list[sqlite3.Row]:
    """ Convenience shortcut: opens a connection, runs a SELECT query, and closes the connection.
        Ideal for simple queries where you don't need to manage the connection."""
    with connect_db() as conn:
        return execute_query(conn,sql,params)
    


_logger: logging.Logger | None = None
def _get_logger()-> logging.Logger:
    """
    """
    global _logger
    if _logger is not None:
        return _logger
    log_dir: Path = get_log_dir()
    log_dir.mkdir(parents=True,exist_ok=True)

    logger = logging.getLogger("core.db")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_file = log_dir /"app.log"
    handler = RotatingFileHandler(
        log_file,
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8"
    )
    fmt = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                             datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(fmt)

    if not logger.handlers:
        logger.addHandler(handler)

    _logger = logger
    return logger

#============================
def apply_pragmas(conn:sqlite3.Connection)-> None:


    s=get_settings()
    lg = _get_logger()
    lg.debug("Applying database pragmas: busy_timeout_ms=%d, journal=%s, synch=%s",
             getattr(s,"db_journal_mode",None),
             getattr(s,"db_synchronous",None),
             getattr(s,"db_busy_timeout_ms",None)
                )
    
    conn.execute(f"PRAGMA busy_timeout =?;", (s.db_busy_timeout_ms,))
    conn.execute(f"PRAGMA journal_mode = {s.db_journal_mode}")
    conn.execute(f"PRAGMA synchronous = {s.db_synchronous}")
    conn.execute("PRAGMA foreign_keys = ON;") #Enable foreign key constraints
    conn.execute("PRAGMA temp_store = MEMORY;") #Use memory for temp storage

#============================
#Target version of the app schema. Increment when there are structural changes.
SCHEMA_VERSION: int = 1

def _get_user_version(conn: sqlite3.Connection) -> int:
    """
        Read the PRAGMA user_version (an integer that versions the schema in the .db file itself).
    """
    row = conn.execute("PRAGMA user_version;").fetchone()
    return int(row[0]) if row else 0

def _set_user_version(conn:sqlite3.Connection, v:int) -> None:
    """
        Set PRAGMA user_version to the specified version (after successful migration).
    """
    conn.execute(f"PRAGMA user_version = {v};")

#Each function migrates from the state (version-1) to the 'version' indicated in the key.
MIGRATIONS: dict[int,Callable[[sqlite3.Connection],None]]= {}

def _migration_1_create_baseline(conn: sqlite3.Connection) -> None:
    """  
        Use IF NOT EXISTS to ensure idempotency in databases that have already been created manually.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            dt          TEXT NOT NULL,           -- ISO date (YYYY-MM-DD) ou datetime
            category    TEXT NOT NULL,
            subcategory TEXT,
            amount      REAL NOT NULL,
            note        TEXT
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_expenses_dt ON expenses(dt);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_expenses_category ON expenses(category);")

MIGRATIONS[1] = _migration_1_create_baseline



def _migrate(conn: sqlite3.Connection, target_version: int) -> None:
    """
        Applies incremental migrations up to 'target_version'.
        Each step runs in a transaction: success → PRAGMA user_version=N; failure → rollback.
    """
    lg = _get_logger()
    current = _get_user_version(conn)
    if current > target_version:
        raise RuntimeError(
            f"DB on version {current} > cod expect {target_version}. "
            "Update the app or handle downgrades with caution."
        )
    if current == target_version:
        lg.info("Schema up-to-date (v%s).", current)
        return

    for nxt in range(current + 1, target_version + 1):
        mig = MIGRATIONS.get(nxt)
        if mig is None:
            raise RuntimeError(f"There is no migration registered to reach this version. {nxt}.")
        lg.info("Applying migration %s → %s ...", nxt-1, nxt)
        
        with transaction(conn, begin_mode="DEFERRED"):
            mig(conn)                 
            _set_user_version(conn, nxt)  
        lg.info("Migration for v%s done.", nxt)


def ensure_schema() -> None:
    """
    Ensures that the .db file is at the expected version (SCHEMA_VERSION). 
        - Opens connection
        - Reads user_version
        - Applies missing migrations
    """
    with connect_db() as conn:
        _migrate(conn, SCHEMA_VERSION)




if __name__ == "__main__":
   # Exemplo: apenas ping/criação
   ensure_db_ready(touch=True)
   # 1) Garante arquivo + acesso
   ensure_db_ready(touch=True)
   # 2) Garante schema na versão alvo
   ensure_schema()
   print("Schema ok na versão:", SCHEMA_VERSION)