from core.config import get_settings,get_db_path
from pathlib import Path


# Auxiliary function to check if a directory is writable
def is_writable_dir(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)         
        test = path / ".__w_test.tmp"                    
        test.write_bytes(b"ok")                         
        test.unlink()                                   
        return True
    except Exception:
        return False

# Main diagnostic function
def main() -> None:
    try:
        s = get_settings()                              
    except Exception as e:
        print("❌ Error loading settings:\n", e)        
        return

    #extract relevant paths
    db_local_dir  = s.db_local_dir
    db_local_path = get_db_path()
    db_backup_dir = s.db_backup_dir
    log_dir       = s.log_dir
    db_remote     = s.db_remote_path

    # Print snapshot
    print("\n=== CONFIG SNAPSHOT ===")
    print(f"Remote .db (Dropbox): {db_remote}")

    print(f"Local dir          : {db_local_dir}")
    print(f"  - exists?        : {db_local_dir.exists()}")
    print(f"  - is_dir?        : {db_local_dir.is_dir()}")
    print(f"  - writable?      : {is_writable_dir(db_local_dir)}")

    print(f"Local .db path     : {db_local_path}")
    print(f"  - parent exists? : {db_local_path.parent.exists()}")
    print(f"  - file exists?   : {db_local_path.exists()}")

    print(f"Backup dir         : {db_backup_dir}")
    if db_backup_dir:
        print(f"  - exists?        : {db_backup_dir.exists()}")
        print(f"  - is_dir?        : {db_backup_dir.is_dir()}")
        print(f"  - writable?      : {is_writable_dir(db_backup_dir)}")

    print(f"Log dir            : {log_dir}")
    if log_dir:
        print(f"  - exists?        : {log_dir.exists()}")
        print(f"  - is_dir?        : {log_dir.is_dir()}")
        print(f"  - writable?      : {is_writable_dir(log_dir)}")

if __name__ == "__main__":
    main()