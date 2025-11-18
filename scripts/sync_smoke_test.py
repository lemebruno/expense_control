

"""
Sync smoke test: pull -> DB ops -> push (Dropbox).
Run:
  python scripts/sync_smoke_test.py
Requirements:
  - Valid .env with DROPBOX_TOKEN, DB_REMOTE_PATH and DB_NAME.
"""
from __future__ import annotations

import os, sys
from datetime import date

# Ensure project root on sys.path (when running from scripts/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.sync_cycle import sync_before_db_use, sync_after_db_use  # pull/ensure/push
from core import repo_expense as repo
from core.models import Expense
from core.db import ensure_db_ready, ensure_schema


def main() -> None:
    # 1) Pull & ensure schema
    print("[1/4] sync_before_db_use() ...")
    sync_before_db_use()
    # (Already ensures file+schema; keep these for explicitness)
    ensure_db_ready(touch=True)
    ensure_schema()
    print("      -> OK (pulled if newer, file+schema ready)")

    # 2) Do a small DB operation (insert + readback)
    print("[2/4] Repository operations ...")
    today = date.today().strftime("%Y-%m-%d")
    new_id = repo.insert(
        Expense(dt=today, category="Food", subcategory="Groceries", amount=9.99, note="sync test")
    )
    got = repo.get_by_id(new_id)
    assert got is not None and got.amount == 9.99
    print(f"      -> Inserted id={new_id}, verified readback (amount=9.99).")

    # 3) Optional: a small aggregation just to touch read paths
    ym_totals = dict(repo.sum_by_month(int(today[:4])))
    print(f"[3/4] sum_by_month({today[:4]}) -> {ym_totals}")

    # 4) Push changes back to Dropbox
    print("[4/4] sync_after_db_use() ...")
    uploaded = sync_after_db_use()
    print(f"      -> Upload performed? {uploaded}")

    print("\nSmoke test completed successfully.")


if __name__ == "__main__":
    main()
