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


#This module handles application configuration settings,
#loading them from environment variables (with optional .env support),
#validating them, and providing a typed interface for access.



#The Settings dataclass holds all configuration settings

@dataclass(frozen=True) #imutable
class Settings:
    SUPABASE_URL: str
    SUPABASE_KEY: str
    # Optional settings
    SUPABASE_DB_URL: str | None = None
    DB_SCHEMA: str = "public"
    LOG_DIR: Path | None = None

    @property
    def supabase_url(self) -> str:
        return self.SUPABASE_URL

    @property
    def supabase_key(self) -> str:
        return self.SUPABASE_KEY

    @property
    def supabase_db_url(self) -> Optional[str]:
        return self.SUPABASE_DB_URL

    @property
    def db_schema(self) -> str:
        return self.DB_SCHEMA

    @property
    def log_dir(self) -> Optional[Path]:
        return self.LOG_DIR
    
    

def _load_env_if_present() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.warning("Python-dotenv not installed; passing .env loading.")

def _read_env_raw() -> dict[str,str]:
    """Reads environment variables already with .env applied and returns raw values (strings)."""
    
    _load_env_if_present() #trying env

    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_KEY", "")
    supabase_db_url = os.environ.get("SUPABASE_DB_URL", "")
    log_dir = os.environ.get("LOG_DIR", "")
    return {
        "SUPABASE_URL": supabase_url,
        "SUPABASE_KEY": supabase_key,
        "SUPABASE_DB_URL": supabase_db_url,
        "LOG_DIR": log_dir,
    } 

def _build_settings(env: dict[str,str]) -> Settings:
    """
    Builds the Settings dataclass instance from raw environment variables.
    Performs normalization and validation of each setting.
    """
    supabase_url = (env.get("SUPABASE_URL") or "").strip()
    supabase_key = (env.get("SUPABASE_KEY") or "").strip()
    supabase_db_url = (env.get("SUPABASE_DB_URL") or "").strip() or None

    # Log directory: if not provided, create a sensible default (~/.expensecontrol/logs)
    raw_log_dir = (env.get("LOG_DIR") or "").strip()
    if raw_log_dir:
        log_dir = Path(raw_log_dir).expanduser().resolve()
    else:
        log_dir = Path.home() / ".expensecontrol" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    settings = Settings(
        SUPABASE_URL=supabase_url,
        SUPABASE_KEY=supabase_key,
        SUPABASE_DB_URL=supabase_db_url,
        LOG_DIR=log_dir
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




def get_log_dir() -> Path:
    """
    Returns log folder
    """
    return get_settings().log_dir



def _validate_required(settings: Settings) -> None:
    # Garantir que SUPABASE_URL e SUPABASE_KEY estejam configurados
    if not (settings.supabase_url or "").strip():
        raise ValueError(
            "SUPABASE_URL is not set. Please provide the Supabase project URL in the .env file."
        )
    if not (settings.supabase_key or "").strip():
        raise ValueError(
            "SUPABASE_KEY is not set. Please provide the Supabase API key in the .env file."
        )
    
  


    


