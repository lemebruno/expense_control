"""
    Dropbox API access layer for the Expense Control app.

    Responsibilities of this module:
    -Initialize the Dropbox client using the DROPBOX_TOKEN from configuration.
    -Provide a function to inspect the remote .db file and
     return important metadata (rev, content_has,size,timestamps).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import logging
from pathlib import Path
from typing import Optional
import shutil
import time

import dropbox
from dropbox.exceptions import AuthError, ApiError
from dropbox.files import FileMetadata, WriteMode

from core.config import get_settings
logger = logging.getLogger(__name__)




MAX_BACKUPS = 3  # maximum number of backup files to keep per DB


def _ensure_backup_dir() -> Path:
    """
    Ensure that the backup directory exists and return it.
    Uses DB_BACKUP_DIR from settings, or falls back to a 'backups'
    folder under the local DB directory.
    """
    settings = get_settings()
    backup_dir = settings.db_backup_dir or (settings.db_local_dir / "backups")
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir


def _rotate_backups(backup_dir: Path, base_stem: str) -> None:
    """
    Keep at most MAX_BACKUPS backup files for a given database.
    Older backups are deleted.
    """
    pattern = f"{base_stem}_*.bak*"
    backups = sorted(
        backup_dir.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for extra in backups[MAX_BACKUPS:]:
        try:
            extra.unlink()
            logger.info("Removed old backup '%s'.", extra)
        except OSError as exc:
            logger.warning("Failed to remove old backup '%s': %s", extra, exc)


def _make_backup(local: Path) -> Path:
    """
    Create a timestamped backup of the local DB file in the backup dir.
    Returns the backup path.
    """
    backup_dir = _ensure_backup_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{local.stem}_{ts}.bak{local.suffix}"
    backup_path = backup_dir / backup_name
    shutil.copy2(local, backup_path)
    _rotate_backups(backup_dir, local.stem)
    logger.info("Created local backup '%s'.", backup_path)
    return backup_path


def _make_conflicted_copy(local: Path) -> Path:
    """
    Create a special 'conflicted' copy of the local DB file for manual
    inspection/merge when a rev conflict is detected.
    """
    backup_dir = _ensure_backup_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    conflicted_name = f"{local.stem}_conflicted_{ts}{local.suffix}"
    conflicted_path = backup_dir / conflicted_name
    shutil.copy2(local, conflicted_path)
    logger.warning("Stored conflicted local copy at '%s'.", conflicted_path)
    return conflicted_path


@dataclass(frozen=True)
class RemoteFileInfo:
    """
        Represents the relevant metadata of a remote file in Dropbox.

        This is intentionally minimal: only the fields needed for sync
         decisions(e.g. pull_if_newer, push_with_rev) are included.

    """

    path:str
    rev:str
    content_hash:str
    size: int
    client_modified:datetime
    server_modified: datetime




def _sidecar_paths(local: Path) -> tuple [Path,Path]:
    """
        
        Build paths for the sidecar files that store the last known
        remote rev and content_hash for the local .db file.

        Example:
            exmpense.db -> expense.db.rev / expense.db.hash
    """

    rev_path = local.with_suffix(local.suffix +".rev")
    hash_path = local.with_suffix(local.suffix + ".hash")
    return rev_path,hash_path

def _read_sidecar(path: Path) -> Optional[str]:
    """

        Read a small text value from a sidecar file.
        Returns None if the filme does not exist or is empty.

    """

    try:
        if not path.exists():
            return None
        
        text = path.read_text(encoding="utf-8").strip()
        return text or None
    
    except OSError:
        #If we cannot read it for some reason, treat as "no value".
        return None
    
def _write_sidecar(path: Path, value: str) -> None:
    """
        
        Write a small text value to a sidecar file.
        Overwrites any previous content.

    """
    try:
        path.write_text(value,encoding="utf-8")
    except OSError as exc:
        #This should not be fatal for the application itself, but
        # we log it because it affects sync decisions in the future.
        logger.warning(
            "Failed to write sidecar file'%s':%s"
            , path
            , exc
        )



@lru_cache(maxsize=1)
def get_dbx() ->dropbox.Dropbox:
    """
        Return a singleton instance of the Dropbox client.

        -Reads the token from configuration (DROPBOX_TOKEN).
        -Performs a lightweight authentication check on first call.
        -Raises RuntimeError if the token is missing or invalid.
    """

    settings = get_settings()
    token = (settings.dropbox_token or "").strip()

    if not token:
        raise RuntimeError(
            "DROPBOX_TOKEN is not configured."
            "Set the environment variable or the value in your .env file."
        )
    
    logger.info("Initializing Dropbox client.")
    #timeout is defensive to avoid hanging forever on network issues
    dbx = dropbox.Dropbox(token,timeout=20)

    try:
        #Validate the token on first use. This call is inexpensive and
        #fails fast if the token is wrong or revoked.
        dbx.users_get_current_account()
        logger.info("Dropbox authentication succeeded")
    except AuthError as exc:
        logger.error("Dropbox authentication error: %s",exc)
        raise RuntimeError(
            "Failed to authenticate with Dropbox."
            "Check if DROPBOX_TOKEN is valid"
        ) from exc
    
    return dbx


def _default_remote_path() ->str:
    """
        Returns the default remote path for the .db file based on settings.

        The value comes from DB_REMOTE_PATH / DB_NAME normalization
        performed in core.config.
    """

    settings = get_settings()
    return settings.db_remote_path

def probe_remote(remote_path: Optional[str]=None) -> Optional[RemoteFileInfo]:
    """
        Fetch metadata for the remote file in Dropbox.

        Args:
            remote_path: Optional Dropbox path. If none, uses the default databse path
            from configuration

        Returns:
            -RemoteFileInfo if the filme exists.
            -None if the file does not exists yet in Dropbox.
        
            Raises:
                RuntimeError for Authentication issues or non-"not found" API errors.
    """

    path = remote_path or _default_remote_path()
    dbx = get_dbx()

    logger.info("Querying Remote filme metadata at '%s'.", path)

    try:
        md = dbx.files_get_metadata(path)
    except ApiError as exc:
        #Typical path for "file does not exist yet in Dropbox".
        #We try to inspect the structured error in a defensive way.
        try:
            error = exc.error
            if hasattr(error,"is_path") and error.is_path():
                logger.info(
                    "Remote file not found at '%s' (it probably does not exist yet).",
                    path,
                )
                return None
        except Exception: # pragma: no cover - defensive branch
            # If we fail to instrospect the error, we fall through to
            # the generic handler below.
            pass
        logger.error(
            "Error while fetching metadata for remote file '%s':%s",path,exc
        )
        raise RuntimeError(
            f"Failed tot fetch Dropbox metadata from '{path}'."
        ) from exc
    
    if not isinstance (md,FileMetadata):
        #The path might refer to a folder or something unexpected.
        logger.warning(
            "Remote path '%s' is not a file (returned type: %s).",
            path, type(md),
        )
        return None
    
    info = RemoteFileInfo(
        path = md.path_display or md.path_lower or path,
        rev = md.rev,
        content_hash = md.content_hash,
        size = md.size,
        client_modified= md.client_modified,
        server_modified= md.server_modified,
    )

    logger.info(
        "Remote metadata: rev=%s size=%d content_hash=%s",
        info.rev,
        info.size,
        info.content_hash,
    )

    return info



def pull_if_newer(
        remote_path: Optional[str] = None,
        local_path: Optional[Path] = None,

) ->bool:
    """
        Download the remote .db file from Dropbox *only if*
        it is newer (different rev/hash) than the last known version.
        Args:
            remote_path: Optional Dropbox file path. If None, uses the
                         default DB path from configuration.
            local_path: Optional local Path for the .db file. If None,
                        uses the default DB local path from configuration.

        Returns:
            True  -> a download was performed (local file updated).
            False -> no download was needed (already up-to-date or no
                     remote file exists).

        Raises:
            RuntimeError for Dropbox API errors other than "file not found".

    """
    t0 = time.perf_counter()
    settings = get_settings()

    remote = remote_path or settings.db_remote_path
    local = local_path or settings.db_local_path

    #ensure the local directory exists.
    local = Path(local)
    local.parent.mkdir(parents=True, exist_ok=True)

    #Inspect the remote file.
    remote_info = probe_remote(remote)
    if remote_info is None:
        # No remote file yet: nothing to pull.
        elapsed = time.perf_counter() - t0
        logger.info(
            "pull_if_newer(): no remote DB at '%s'. duration=%.3fs",
            remote,
            elapsed,
        )
        return False
    
    rev_path,hash_path = _sidecar_paths(local)
    current_rev = _read_sidecar(rev_path)
    current_hash = _read_sidecar(hash_path)

    #If we have a local file and the sidecar matches the current
    # remote metadata, we assume the local copy is already up_to_date.
    if (
        local.exists()
        and current_rev == remote_info.rev
        and current_hash == remote_info.content_hash
    ):
        elapsed = time.perf_counter() - t0
        logger.info(
            "pull_if_newer(): local DB up-to-date. rev=%s size=%d duration=%.3fs",
            remote_info.rev,
            remote_info.size,
            elapsed,
        )
        return False

    #Otherwise, download the remote file and update sidecar metadata.
    logger.info(
        "Pulling newer remote database from '%s' (rev=%s, size=%d bytes).",
        remote,
        remote_info.rev,
        remote_info.size,
    )

    dbx = get_dbx()
    try:
        dbx.files_download_to_file(str(local), remote)
    except ApiError as exc:
        elapsed = time.perf_counter() - t0
        logger.error(
            "Failed to download remote database '%s' after %.3fs: %s",
            remote,
            elapsed,
            exc,
        )
        raise RuntimeError(
            f"Failed to download remote database from '{remote}'."
        ) from exc

    _write_sidecar(rev_path, remote_info.rev)
    _write_sidecar(hash_path, remote_info.content_hash)

    elapsed = time.perf_counter() - t0
    logger.info(
        "Local database updated from remote rev=%s (bytes=%d, duration=%.3fs).",
        remote_info.rev,
        remote_info.size,
        elapsed,
    )
    return True


def push_with_rev(
    remote_path: Optional[str] = None,
    local_path: Optional[Path] = None,
) -> bool:
    """
    Upload the local .db file to Dropbox, using rev-based optimistic
    concurrency to avoid overwriting changes from another machine.

    Behaviour:
      - If there is no remote file yet, it creates a new one (WriteMode.add).
      - If there is a remote file and the local sidecar rev matches the
        current remote rev, it uploads with WriteMode.update(rev).
      - If revs diverge (sidecar != remote.rev), it DOES NOT overwrite:
        it logs a conflict and returns False.

    Args:
        remote_path: Optional Dropbox file path. If None, uses the
                     default DB path from configuration.
        local_path: Optional local Path for the .db file. If None,
                    uses the default DB local path from configuration.

    Returns:
        True  -> upload performed successfully and sidecars updated.
        False -> upload skipped due to conflict or missing remote/local
                 conditions (e.g., conflict on rev).

    Raises:
        FileNotFoundError if the local .db file does not exist.
        RuntimeError for Dropbox API errors other than rev conflicts.
    """
    t0 = time.perf_counter()
    settings = get_settings()

    remote = remote_path or settings.db_remote_path
    local = local_path or settings.db_local_path
    local = Path(local)

    if not local.exists():
        elapsed = time.perf_counter() - t0
        logger.error(
            "push_with_rev(): local DB does not exist (%s). duration=%.3fs",
            local,
            elapsed,
        )
        raise FileNotFoundError(
            f"Local database file does not exist: {local}"
        )
    # Fail-safe: create a local backup before attempting any upload.
    try:
        _make_backup(local)
    except Exception as exc:
        # Backup failure should not block the upload, but must be logged.
        logger.warning(
            "Failed to create pre-push backup for '%s': %s",
            local,
            exc,
        )
    

    rev_path, hash_path = _sidecar_paths(local)
    current_rev = _read_sidecar(rev_path)
    current_hash = _read_sidecar(hash_path)

    # Inspect current remote state.
    remote_info = probe_remote(remote)

    dbx = get_dbx()

    # Case 1: no remote file yet -> first upload (ADD).
    if remote_info is None:
        logger.info(
            "No remote database found at '%s'. Performing first upload (ADD).",
            remote,
        )
        with local.open("rb") as f:
            try:
                data = f.read()
                md = dbx.files_upload(
                    data,
                    remote,
                    mode=WriteMode.add,
                    mute=True,
                )
            except ApiError as exc:
                logger.error("Failed to upload new database to '%s': %s", remote, exc)
                raise RuntimeError(
                    f"Failed to upload new database to '{remote}'."
                ) from exc

        _write_sidecar(rev_path, md.rev)
        _write_sidecar(hash_path, md.content_hash)
        elapsed = time.perf_counter() - t0
        logger.info(
            "First upload completed. New rev=%s bytes=%d duration=%.3fs.",
            md.rev,
            md.size,
            elapsed,
        )
        return True

    # From here on, remote_info is not None (file exists in Dropbox).
    logger.info(
        "Remote database exists at '%s' with rev=%s.",
        remote,
        remote_info.rev,
    )

    # If we don't have a local rev, we cannot safely decide about overwriting.
    if current_rev is None:
        elapsed = time.perf_counter() - t0
        logger.warning(
            "Rev conflict detected for '%s': local rev=%s, remote rev=%s. "
            "Upload will NOT overwrite remote file. duration=%.3fs",
            remote,
            current_rev,
            remote_info.rev,
            elapsed,
        )
        return False

    # If revs diverge, there is a conflict: someone else updated the file.
    if current_rev != remote_info.rev:
        logger.warning(
            "Rev conflict detected for '%s': local rev=%s, remote rev=%s. "
            "Upload will NOT overwrite remote file.",
            remote,
            current_rev,
            remote_info.rev,
        )
        # Keep a conflicted copy of the local DB for manual inspection.
        try:
            _make_conflicted_copy(local)
        except Exception as exc:
            logger.warning(
                "Failed to create conflicted copy for '%s': %s",
                local,
                exc,
            )
        return False

    # Safe path: local sidecar rev matches current remote rev -> use update.
    logger.info(
        "Uploading local database to '%s' with WriteMode.update(rev=%s).",
        remote,
        current_rev,
    )

    with local.open("rb") as f:
        try:
            md = dbx.files_upload(
                f.read(),
                remote,
                mode=WriteMode.update(current_rev),
                mute=True,
            )
        except ApiError as exc:
            # Check if the error is a rev conflict that happened between
            # probe_remote() and files_upload(). If so, we log and return False.
            try:
                error = exc.error
                if hasattr(error, "is_path") and error.is_path():
                    path_error = error.get_path()
                    if hasattr(path_error, "is_conflict") and path_error.is_conflict():
                        logger.warning(
                            "Rev conflict during upload to '%s'. "
                            "Remote was modified concurrently. Upload aborted.",
                            remote,
                        )
                        try:
                            _make_conflicted_copy(local)
                        except Exception as exc2:
                            logger.warning(
                                "Failed to create conflicted copy after concurrent "
                                "update for '%s': %s",
                                local,
                                exc2,
                            )
                        # In case of conflict, do NOT overwrite; just return False.
                        return False
            except Exception:
                # If error inspection fails, fall through to generic handler.
                pass

            logger.error(
                "Failed to upload database to '%s' with update(rev=%s): %s",
                remote,
                current_rev,
                exc,
            )
            raise RuntimeError(
                f"Failed to upload database to '{remote}' with rev={current_rev}."
            ) from exc

    # Upload succeeded. Update sidecars with the new metadata.
    _write_sidecar(rev_path, md.rev)
    _write_sidecar(hash_path, md.content_hash)

    elapsed = time.perf_counter() - t0
    logger.info(
        "Upload completed successfully. Old rev=%s, new rev=%s, bytes=%d, duration=%.3fs.",
        current_rev,
        md.rev,
        md.size,
        elapsed,
    )
    return True



