
"""
    core.sync_cycle
    
    High-level synchronization cycle helpers for Dropbox + SQLite.
    
    This module orchestrates the typical flow:
    
      - Before using the database:
            sync_before_db_use()
            -> pull_if_newer()
            -> ensure_db_ready() + ensure_schema()
    
      - After using the database (on app exit or after critical operations):
            sync_after_db_use()
            -> push_with_rev()
    
    The idea is that the GUI or any entry-point script calls these functions
    instead of dealing directly with Dropbox / DB orchestration details.
"""

from __future__ import annotations

import logging
import time

from core.db import ensure_db_ready, ensure_schema
from core.storage_dropbox import pull_if_newer, push_with_rev


logger = logging.getLogger(__name__)


def sync_before_db_use() -> None:
    """
    Run the "startup" sync sequence:

      1) pull_if_newer() from Dropbox (if remote exists and is newer).
      2) ensure_db_ready(touch=True) to create/ping the local file.
      3) ensure_schema() to make sure the schema is at target version.

    This function is intended to be called once when the application
    starts, before the first database connection is used.
    """
    t0 = time.perf_counter()
    logger.info("Starting sync_before_db_use() cycle.")

    # Step 1: Pull from Dropbox if needed.
    try:
        updated = pull_if_newer()
        logger.info("pull_if_newer() completed. updated=%s", updated)
    except Exception as exc:
        logger.error("Error during pull_if_newer(): %s", exc)
        # Depending on the app policy, we might choose to continue or abort.
        # For now, we re-raise so that callers can decide.
        raise

    # Step 2: Ensure local DB exists and is writable.
    ensure_db_ready(touch=True)

    # Step 3: Ensure schema is up-to-date.
    ensure_schema()

    elapsed = time.perf_counter() - t0
    logger.info(
        "sync_before_db_use() finished in %.3f seconds (updated=%s).",
        elapsed,
        updated,
    )


def sync_after_db_use() -> bool:
    """
    Run the "shutdown" sync sequence:

      1) push_with_rev() to upload local changes to Dropbox using
         rev-based optimistic concurrency.

    Returns:
        True  -> upload performed successfully.
        False -> upload skipped (e.g., rev conflict or missing sidecar).

    This function is intended to be called when the application is about
    to exit, or after critical operations that must be persisted remotely.
    """
    t0 = time.perf_counter()
    logger.info("Starting sync_after_db_use() cycle.")

    try:
        result = push_with_rev()
    except Exception as exc:
        logger.error("Error during push_with_rev(): %s", exc)
        raise

    elapsed = time.perf_counter() - t0
    logger.info(
        "sync_after_db_use() finished in %.3f seconds (upload_performed=%s).",
        elapsed,
        result,
    )
    return result
