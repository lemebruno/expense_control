#%%

from dataclasses import dataclass
from pathlib import Path
import logging
logger = logging.getLogger(__name__)
import os
import re



@dataclass(frozen=True) #imutable

class Settings:
    DROPBOX_TOKEN:str
    DB_REMOTE_PATH:str
    DB_NAME: str
    DB_LOCAL_DIR: Path
    DB_LOCAL_PATH: Path
    DB_BACKUP_DIR:Path | None = None
    journal_mode: str = "DELETE" # To facilitate transactions on cloud
    busy_timeout_ms: int = 5000 # Just for precaution
    synchronous: str = "NORMAL"

def _load_env_if_present() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.warning("Python-dotenv not installed; passing .env loading.")

def _read_env_raw() -> dict[str,str]:
    """Reads environment variables already with .env applied and returns raw values (strings)."""
    
    _load_env_if_present #trying env

    token = os.environ.get("DROPBOX_TOKEN","")
    remote = os.environ.get("DB_REMOTE_PATH","")
    name = os.environ.get("DB_NAME","")
    local = os.environ.get("DB_LOCAL_DIR","")
    backup = os.environ.get("DB_LOCAL_PATH","")

    #OPTIONAL
    journal = os.environ.get("journal_mode","")
    busy = os.environ.get("busy_timeout_ms","")
    sync = os.environ.get("synchronous","")

    return{
        "DROPBOX_TOKEN": token,
        "DB_REMOTE_PATH": remote,
        "DB_NAME": name,
        "DB_LOCAL_DIR": local,
        "DB_LOCAL_PATH": backup,
        "journal_mode": journal,
        "busy_timeout_ms": busy,
        "synchronous": sync
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


errs = [
    ("/apenas/diretorio/",""),
    ("/expense/../segredo","data"),
    ("/./expense","data"),
]
for r,n in errs:
    try:
        _normalize_remote_path(r,n)
    except ValueError as e:
        print("ERR OK:", r,n,"->", e)





