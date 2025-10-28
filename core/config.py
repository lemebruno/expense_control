#%%

from dataclasses import dataclass
from pathlib import Path
import logging
logger = logging.getLogger(__name__)
import os
import re
import uuid # for unique temp file names
from typing import Optional # For type hinting optional values



@dataclass(frozen=True) #imutable

class Settings:
    DROPBOX_TOKEN:str
    DB_REMOTE_PATH:str
    DB_NAME: str
    DB_LOCAL_DIR: Path
    DB_BACKUP_DIR: Path
    DB_BACKUP_DIR:Path | None = None
    DB_JOURNAL_MODE: str = "DELETE" # To facilitate transactions on cloud
    DB_BUSY_TIMEOUT_MS: int = 5000 # Just for precaution
    DB_SYNCHRONOUS: str = "NORMAL"

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
            f"DB_BACKUP_DIR path is not writable: {dir_path} | Error: {e}"
            "\nPlease provide a writable directory.Or set up another path in DB_BACKUP_DIR environment variable."
            ) from e



def _resolve_local_dir(raw_local:str) ->Path:
    """
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




if __name__ == "__main__":
    env = _read_env_raw()
    local_dir = _resolve_local_dir(env.get("DB_LOCAL_DIR",""))
    print("Local dir resolved to:",local_dir)

    