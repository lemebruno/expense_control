
"""
Manual test for the high-level sync cycle.

How to run:
    python -m tests.test_sync_cycle

This will:
  1) Run sync_before_db_use()  -> pull from Dropbox + ensure DB/schema.
  2) (Optionally) simulate some work.
  3) Run sync_after_db_use()   -> push local DB to Dropbox.
"""

from __future__ import annotations

from pathlib import Path

from core.config import get_settings
from core.sync_cycle import sync_before_db_use, sync_after_db_use


def main() -> None:
    settings = get_settings()
    local = Path(settings.db_local_path)
    remote = settings.db_remote_path

    print("=== Sync cycle test ===")
    print(f"Remote DB path (Dropbox): {remote}")
    print(f"Local  DB path         : {local}")
    print()

    # 1) Before DB use
    print("-> Running sync_before_db_use() ...")
    try:
        sync_before_db_use()
    except Exception as exc:
        print("ERROR during sync_before_db_use():", exc)
        return

    print("sync_before_db_use() completed.")
    print(f"Local exists after sync? {local.exists()}")
    print()

    # Here we could simulate work with the database, e.g. by calling
    # ensure_db_ready() + ensure_schema() + some inserts/selects via core.db.
    # For now, we just proceed to the final sync.

    # 2) After DB use
    print("-> Running sync_after_db_use() ...")
    try:
        uploaded = sync_after_db_use()
    except Exception as exc:
        print("ERROR during sync_after_db_use():", exc)
        return

    print(f"sync_after_db_use() completed. upload_performed = {uploaded}")
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
