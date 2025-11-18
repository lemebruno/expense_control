"""
    core.sync_cycle

    High-level startup/shutdown helpers for a PostgreSQL (Supabase) backend.

    Typical flow:

      - Before using the database:
            sync_before_db_use()
            -> ensure_db_ready() + ensure_schema()

      - After using the database:
            sync_after_db_use()
            -> no-op (changes are already persisted by transactions)

    The idea is that the GUI or any entry-point script calls these functions
    to prepare the DB and keep orchestration separate from the UI.
"""

from __future__ import annotations

import logging
import time

from .db import ensure_db_ready, ensure_schema


logger = logging.getLogger(__name__)


def sync_before_db_use() -> None:
    """
    Run the startup sequence for a PostgreSQL (Supabase) backend:
      1) ensure_db_ready(touch=True) to check connectivity and write capability
      2) ensure_schema() to apply pending migrations
    This function is intended to be called once when the application starts.
    """
    t0 = time.perf_counter()
    logger.info("Starting sync_before_db_use() cycle.")

    # Step 1: Ensure DB is reachable and writable.
    ensure_db_ready(touch=True)

    # Step 2: Ensure schema is up to date.
    ensure_schema()

    elapsed = time.perf_counter() - t0
    logger.info("sync_before_db_use() finished in %.3f seconds.", elapsed)


def sync_after_db_use() -> bool:
    """
    No-op for PostgreSQL (Supabase). Transactions already persisted changes.
    This function exists to keep a stable API for the GUI/app entry-points.
    """
    t0 = time.perf_counter()
    logger.info("Starting sync_after_db_use() cycle.")

    elapsed = time.perf_counter() - t0
    logger.info("sync_after_db_use() finished in %.3f seconds.", elapsed)
    return True
