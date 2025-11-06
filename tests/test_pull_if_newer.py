"""
Manual test runner for Dropbox pull_if_newer.

How to run:
    python -m tests.test_pull_if_newer

Preconditions:
  - DROPBOX_TOKEN is valid (configured in .env / environment).
  - DB_REMOTE_PATH / DB_NAME point to the correct .db path in Dropbox.
"""

from __future__ import annotations

from pathlib import Path

from core.config import get_settings
from core.storage_dropbox import probe_remote, pull_if_newer


def _read_sidecar(path: Path) -> str:
    """
    Small helper to show sidecar contents on screen.
    Returns 'N/A' if the file does not exist.
    """
    try:
        if not path.exists():
            return "N/A (file not found)"
        text = path.read_text(encoding="utf-8").strip()
        return text or "N/A (empty)"
    except OSError as exc:
        return f"Error reading sidecar: {exc}"


def main() -> None:
    settings = get_settings()
    local = Path(settings.db_local_path)
    remote = settings.db_remote_path

    print("=== Dropbox pull_if_newer test ===")
    print(f"Remote DB path (Dropbox): {remote}")
    print(f"Local  DB path         : {local}")
    print()

    # 1) Probe remote metadata
    print("-> Probing remote file...")
    info = probe_remote()
    if info is None:
        print("Remote file does NOT exist yet in Dropbox.")
    else:
        print("Remote file found:")
        print(f"  path           : {info.path}")
        print(f"  rev            : {info.rev}")
        print(f"  content_hash   : {info.content_hash}")
        print(f"  size (bytes)   : {info.size}")
        print(f"  client_modified: {info.client_modified}")
        print(f"  server_modified: {info.server_modified}")
    print()

    # 2) Show current local state before pull
    print("-> Local state BEFORE pull:")
    print(f"  Local exists? : {local.exists()}")
    rev_sidecar = local.with_suffix(local.suffix + ".rev")
    hash_sidecar = local.with_suffix(local.suffix + ".hash")
    print(f"  Sidecar .rev  : {rev_sidecar} -> {_read_sidecar(rev_sidecar)}")
    print(f"  Sidecar .hash : {hash_sidecar} -> {_read_sidecar(hash_sidecar)}")
    print()

    # 3) Execute pull_if_newer
    print("-> Calling pull_if_newer() ...")
    try:
        updated = pull_if_newer()
    except Exception as exc:
        print("ERROR while running pull_if_newer():", exc)
        return

    print(f"pull_if_newer() result -> updated = {updated}")
    print()

    # 4) Show local state after pull
    print("-> Local state AFTER pull:")
    print(f"  Local exists? : {local.exists()}")
    print(f"  Sidecar .rev  : {rev_sidecar} -> {_read_sidecar(rev_sidecar)}")
    print(f"  Sidecar .hash : {hash_sidecar} -> {_read_sidecar(hash_sidecar)}")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()