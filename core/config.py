#%%

from dataclasses import dataclass
from pathlib import Path
import logging
logger = logging.getLogger(__name__)
import os
import re
import uuid # for unique temp file names
from typing import Optional # For type hinting optional values
from typing import TypedDict # For structured dicts
from functools import lru_cache # For caching function results



@dataclass(frozen=True) #imutable

class Settings:
    DROPBOX_TOKEN:str
    DB_REMOTE_PATH:str
    DB_NAME: str
    DB_LOCAL_DIR: Path
    DB_LOCAL_PATH: Path  
    DB_BACKUP_DIR:Path | None = None
    DB_JOURNAL_MODE: str = "DELETE" # To facilitate transactions on cloud
    DB_BUSY_TIMEOUT_MS: int = 5000 # Just for precaution
    DB_SYNCHRONOUS: str = "NORMAL"

    @property
    def dropbox_token(self) -> str:
        return self.DROPBOX_TOKEN
    @property
    def db_remote_path(self) -> str:
        return self.DB_REMOTE_PATH
    @property
    def db_name(self) -> str:
        return self.DB_NAME
    @property
    def db_local_dir(self) -> Path:
        return self.DB_LOCAL_DIR
    @property
    def db_local_path(self) -> Path:
        return self.DB_LOCAL_PATH
    @property
    def db_backup_dir(self) -> Optional[Path]:
        return self.DB_BACKUP_DIR
    @property
    def db_journal_mode(self) -> str:
        return self.DB_JOURNAL_MODE
    @property
    def db_busy_timeout_ms(self) -> int:
        return self.DB_BUSY_TIMEOUT_MS
    @property
    def db_synchronous(self) -> str:
        return self.DB_SYNCHRONOUS
    
    

def _load_env_if_present() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.warning("Python-dotenv not installed; passing .env loading.")

def _read_env_raw() -> dict[str,str]:
    """Reads environment variables already with .env applied and returns raw values (strings)."""
    
    _load_env_if_present() #trying env

    token = os.environ.get("DROPBOX_TOKEN","")
    remote = os.environ.get("DB_REMOTE_PATH","")
    name = os.environ.get("DB_NAME","")
    local = os.environ.get("DB_LOCAL_DIR","")
    backup = os.environ.get("DB_BACKUP_DIR","")

    #OPTIONAL
    journal = os.environ.get("DB_JOURNAL_MODE","")
    busy = os.environ.get("DB_BUSY_TIMEOUT_MS","")
    sync = os.environ.get("DB_SYNCHRONOUS","")

    return{
        "DROPBOX_TOKEN": token,
        "DB_REMOTE_PATH": remote,
        "DB_NAME": name,
        "DB_LOCAL_DIR": local,
        "DB_BACKUP_DIR": backup,
        "DB_JOURNAL_MODE": journal,
        "DB_BUSY_TIMEOUT_MS": busy,
        "DB_SYNCHRONOUS": sync
    } 

def _normalize_remote_path(raw_remote:str,raw_name:str)-> str:
    """Cleans and validates the remote path and name,
      returning the final remote path."""
    remote = (raw_remote or "").strip()
    name = (raw_name or "").strip()

    # Clean remote path
    # Replace backslashes with forward slashes
    remote = remote.replace("\\","/")
    remote = re.sub(r"^/+","/",remote)#guarantees only 1 "/" at the beginning
    if not remote.startswith("/") : remote = "/"+remote # add if it haven't
    remote = re.sub(r"/{2,}","/",remote) #colapse 2 "/" into 1 on the body

    # Validate remote path
    # Check if remote is directory
    
    last = remote.split("/")[-1] if remote else ""
    is_dir = remote.endswith("/") or remote == ""
    if is_dir:
        if not name:
            raise ValueError(
                "DB_REMOTE_PATH is a directory but DB_NAME is empty"
                "Enter DB_NAME or pass the complete file in DB_REMOTE_PATH (ex:/expense/expense.db)"
            )
        remote = f"{remote.rstrip('/')}/{name}"

    # Validate no invalid segments    
    segments = [seg for seg in remote.split("/") if seg]
    if any(seg in (".","..") for seg in segments):
        raise ValueError("DB_REMOTE_PATH contain invalid segments('.' or '..')")

    
    # If not ending with .db, add it
    remote = remote.rstrip("/")
    last = remote.split("/")[-1] if remote else ""
    if not last.lower().endswith(".db"):
        remote = remote+".db"

    return remote






def _assert_writable_dir(dir_path:Path) -> None:
    """Checks if the given directory path is writable by attempting to create and delete a temp file.
     Raises ValueError if not writable.
     """
    tmp_name = f".__write_teste_{uuid.uuid4().hex}__.tmp" #unique temp file name
    tmp_path = dir_path / tmp_name # The / operator joins paths
    try:
        tmp_path.write_bytes(b"ok")

        # If write succeeded, delete the temp file
        tmp_path.unlink() 
    except Exception as e:
        raise ValueError(
            f"Directory is not writable: {dir_path} | Error: {e}"
            "\nProvide a writable folder or set DB_LOCAL_DIR/DB_BACKUP_DIR accordingly."
            ) from e



def _resolve_local_dir(raw_local:str) ->Path:
    """Resolves the local directory path for the database.
    If raw_local is provided, it is used as the path.
    Otherwise, defaults to %LOCALAPPDATA%/ExpenseControl.
    """
    raw = (raw_local or "").strip() #handle None and whitespace

    # Resolve path
    if raw:
        dir_path = Path(raw).expanduser().resolve()
    else:
        base = os.getenv("LOCALAPPDATA")
        if not base:
            base = str(Path.home()/"AppData"/"Local")
        dir_path = Path(base)/ "ExpenseControl"
    
    dir_path.mkdir(parents=True,exist_ok=True) #ensure dir exists and create if not

    _assert_writable_dir(dir_path)

    return dir_path


def _normalize_db_name(raw_name: str) ->str:
    """Cleans and validates the database name, returning the final name.
    """

    name = (raw_name or "").strip()
    if not name:
        raise ValueError("DB_NAME is empty. Please provide a valid database name.(eg. expense.db)")
    if "/" in name or "\\" in name:
        raise ValueError("DB_NAME should not contain path separators ('/' or '\\'). Please provide only the database file name.")
    if not name.lower().endswith(".db"):
        name = name + ".db"
    return name


def _build_db_local_path(local_dir: Path, raw_name:str) ->Path:
    """ Builds the full local database file path by combining the local directory and normalized database name.
        
    """

    name = _normalize_db_name(raw_name)
    return local_dir / name








# TypedDict for pragmas
class Pragmas(TypedDict):
    journal_mode: str # DELETE, WAL, etc.
    synchronous: str  # OFF, NORMAL, FULL, EXTRA
    busy_timeout_ms: int # Time in milliseconds


def _parse_pragmas(env: dict[str,str]) -> Pragmas:
    """
        Parses and validates database pragmas from environment variables.
        Returns a Pragmas TypedDict with validated values.
    """


    # 1. Set defaults
    default_journal = "DELETE"
    default_timeout = 5000
    default_sync = "NORMAL"
    # 2. Read raw values
    raw_journal = env.get("DB_JOURNAL_MODE","").strip()
    raw_timeout = env.get("DB_BUSY_TIMEOUT_MS","").strip()
    raw_sync = env.get("DB_SYNCHRONOUS","").strip()

    # 3. Normalize values
    journal = (raw_journal or default_journal).upper()
    sync = (raw_sync or default_sync).upper()

    # 4.  Validate journal and synchronous values
    valid_journal = {"DELETE","WAL","OFF","TRUNCATE","PERSIST","MEMORY"}
    if journal not in valid_journal:
        raise ValueError(
            f"Invalid DB_JOURNAL_MODE: {journal}." 
            f"Valid options are: {', '.join(valid_journal)}"
            )
    
    valid_sync = {"OFF","NORMAL","FULL","EXTRA"}
    if sync not in valid_sync:
        raise ValueError(
            f"Invalid DB_SYNCHRONOUS: {sync}." 
            f"Valid options are: {', '.join(sorted(valid_sync))}."
            )
    # 5. Convert timeout to int and validate
    if raw_timeout =="":
        timeout_ms = default_timeout
    else:
        try:
            timeout_ms = int(raw_timeout)
            if timeout_ms <0:
                raise ValueError("DB_BUSY_TIMEOUT_MS must be non-negative.")
        except ValueError as e:
            raise ValueError("DB_BUSY_TIMEOUT_MS must be an integer.(eg.: 5000)") from e
    # 6. Return pragmas dict
    return Pragmas(
        journal_mode=journal,
        synchronous=sync,
        busy_timeout_ms=timeout_ms
    )


def _build_settings(env: dict[str,str]) -> Settings:
    """
    Builds the Settings dataclass instance from raw environment variables.
    Performs normalization and validation of each setting.
    """
    #1. Dropbox token(raw string)
    dropbox_token = (env.get("DROPBOX_TOKEN","") or "").strip()

    #2. Remote path(normalized)
    remote_path = _normalize_remote_path(
        env.get("DB_REMOTE_PATH",""),
        env.get("DB_NAME","")
    )

    #3. Local dir(resolved path)
    local_dir = _resolve_local_dir(env.get("DB_LOCAL_DIR",""))

    #4. Local path (combined path)
    db_name = _normalize_db_name(env.get("DB_NAME",""))
    local_path = _build_db_local_path(local_dir, db_name)

    #5. Pragmas (parsed and validated)
    pragmas = _parse_pragmas(env)
    journal_mode = pragmas["journal_mode"]
    busy_timeout_ms = pragmas["busy_timeout_ms"]
    synchronous = pragmas["synchronous"]

    #6. Backup dir (optional, validated path)
    raw_backup = (env.get("DB_BACKUP_DIR","") or "").strip()
    if raw_backup:
        db_backup_dir = Path(raw_backup).expanduser().resolve()
        db_backup_dir.mkdir(parents=True,exist_ok=True)
        _assert_writable_dir(db_backup_dir)
    else:
        db_backup_dir = None

    

    #7. Build and return Settings
    settings = Settings(
        DROPBOX_TOKEN=dropbox_token,
        DB_REMOTE_PATH=remote_path,
        DB_NAME=db_name,
        DB_LOCAL_DIR=local_dir,
        DB_LOCAL_PATH=local_path,
        DB_BACKUP_DIR=db_backup_dir,
        DB_JOURNAL_MODE=journal_mode,
        DB_BUSY_TIMEOUT_MS=busy_timeout_ms,
        DB_SYNCHRONOUS=synchronous
    )
    _validate_required(settings)
    return settings

@lru_cache(maxsize=1) # Cache the settings after first load

def get_settings() ->Settings:
    """
    Public function to get the application settings.
    Returns a Settings dataclass instance with all configuration values.
    """
    env = _read_env_raw()    
    return _build_settings(env)

def refresh_settings() ->None:
    """
    Clears the cached settings, forcing a reload on next get_settings() call.
    """
    get_settings.cache_clear()


def get_db_path() -> Path:
    """
     Public function to get the local database file path.`
    """
    
    return get_settings().DB_LOCAL_PATH


def _validate_required(settings:Settings) ->None:
    """
    Validates that all required settings are properly set.
    Raises ValueError with descriptive messages if any required setting is missing or invalid.
    """

    #1. Validate DROPBOX_TOKEN
    if not (settings.dropbox_token or "").strip():
        raise ValueError(
            "DROPBOX_TOKEN is not set. Please provide a valid Dropbox API token in the DROPBOX_TOKEN environment variable."
        )
    
    #2. Validate DB_NAME and DB_REMOTE_PATH
    if not (settings.db_name or "").strip():
        raise ValueError(
            "DB_NAME is not set. Please provide a valid database name in the DB_NAME environment variable."
        )
    
    #3. Validate DB_REMOTE_PATH
    if not settings.DB_REMOTE_PATH.lower().endswith(".db"):
        raise ValueError(
            "DB_REMOTE_PATH must point to a .db file. Please provide a valid database file path in the DB_REMOTE_PATH environment variable."
        )
    


    
if __name__ == "__main__":
    try:
        s = get_settings()
        print("Settings loaded successfully:",s.DB_LOCAL_PATH)
    except Exception as e:
        print("Error loading settings:",e)
    



