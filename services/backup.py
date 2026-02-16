import gzip
import logging
import shutil
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from prep_brain.config import get_db_path, load_config, resolve_path

logger = logging.getLogger(__name__)

DEFAULT_BACKUP_DIR = resolve_path("data/backups")
LAST_BACKUP_MARKER_FILE = "last_backup_timestamp"

def get_backup_dir() -> Path:
    """Get or create backup directory."""
    config = load_config()
    backup_cfg = config.get("backup", {})
    # If directory is configured, resolve it relative to project root if it's relative
    cfg_dir = backup_cfg.get("directory")
    if cfg_dir:
        backup_dir = resolve_path(cfg_dir)
    else:
        backup_dir = DEFAULT_BACKUP_DIR
        
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir

def get_retention_policy() -> int:
    config = load_config()
    return int(config.get("backup", {}).get("retention_days", 7))

def get_interval_hours() -> int:
    config = load_config()
    return int(config.get("backup", {}).get("interval_hours", 24))

def create_backup(compress: bool = True) -> Path:
    """Create a backup of the SQLite database."""
    db_path = get_db_path()
    backup_dir = get_backup_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = ".db.gz" if compress else ".db"
    backup_name = f"memory_backup_{timestamp}{suffix}"
    backup_path = backup_dir / backup_name

    logger.info(f"Creating backup: {backup_path}")

    # Use SQLite's backup API for consistency
    source_conn = sqlite3.connect(db_path)
    temp_backup = backup_dir / f"memory_backup_{timestamp}.db"

    try:
        # Create backup using SQLite backup API
        backup_conn = sqlite3.connect(temp_backup)
        source_conn.backup(backup_conn)
        backup_conn.close()
        source_conn.close()

        if compress:
            # Compress the backup
            with open(temp_backup, "rb") as f_in:
                with gzip.open(backup_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            temp_backup.unlink()
            logger.info(f"Backup compressed: {backup_path}")
        else:
            shutil.move(temp_backup, backup_path)

        # Update marker
        _update_last_backup_time(backup_dir)

        # Get backup size
        size_mb = backup_path.stat().st_size / (1024 * 1024)
        logger.info(f"Backup complete: {backup_path} ({size_mb:.2f} MB)")

        return backup_path

    except Exception as e:
        logger.error(f"Backup failed: {e}")
        # Cleanup temp file if exists
        if temp_backup.exists():
            temp_backup.unlink()
        raise

def rotate_backups(max_backups: Optional[int] = None) -> int:
    """Remove old backups."""
    if max_backups is None:
        max_backups = get_retention_policy()
        
    backup_dir = get_backup_dir()
    backups = sorted(
        backup_dir.glob("memory_backup_*.db*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    removed = 0
    if len(backups) > max_backups:
        for backup in backups[max_backups:]:
            logger.info(f"Removing old backup (rotation): {backup.name}")
            try:
                backup.unlink()
                removed += 1
            except Exception as e:
                logger.error(f"Failed to remove {backup}: {e}")

    return removed

def list_backups() -> List[Dict]:
    """List all existing backups."""
    backup_dir = get_backup_dir()
    backups = sorted(
        backup_dir.glob("memory_backup_*.db*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    result = []
    for backup in backups:
        stat = backup.stat()
        result.append({
            "path": str(backup),
            "name": backup.name,
            "size_mb": stat.st_size / (1024 * 1024),
            "created": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        })

    return result

def restore_backup(backup_path: Path) -> None:
    """Restore database from a backup."""
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup not found: {backup_path}")

    db_path = get_db_path()

    # Pre-restore backup
    logger.info("Creating pre-restore backup of current database...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    pre_restore_backup = db_path.parent / f"memory_pre_restore_{timestamp}.db"
    
    if db_path.exists():
        shutil.copy2(db_path, pre_restore_backup)
        logger.info(f"Pre-restore backup created: {pre_restore_backup}")

    try:
        if str(backup_path).endswith(".gz"):
            # Decompress and restore
            with gzip.open(backup_path, "rb") as f_in:
                with open(db_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy2(backup_path, db_path)

        logger.info(f"Database restored from: {backup_path}")

    except Exception as e:
        logger.error(f"Restore failed: {e}")
        # Attempt to restore original
        if pre_restore_backup.exists():
            logger.info("Attempting to restore original database...")
            shutil.copy2(pre_restore_backup, db_path)
        raise

def _update_last_backup_time(backup_dir: Path) -> None:
    marker = backup_dir / LAST_BACKUP_MARKER_FILE
    marker.write_text(str(time.time()))

def _get_last_backup_time() -> float:
    backup_dir = get_backup_dir()
    marker = backup_dir / LAST_BACKUP_MARKER_FILE
    if not marker.exists():
        return 0.0
    try:
        return float(marker.read_text().strip())
    except ValueError:
        return 0.0

def run_backup_if_due() -> bool:
    """Checks if backup is due and runs it if so. Returns True if backup ran."""
    config = load_config()
    if not config.get("backup", {}).get("enabled", False):
        return False
        
    last_run = _get_last_backup_time()
    interval_hours = get_interval_hours()
    interval_seconds = interval_hours * 3600
    
    if time.time() - last_run > interval_seconds:
        logger.info(f"Backup due (last run: {datetime.fromtimestamp(last_run)}). Starting backup...")
        try:
            create_backup()
            rotate_backups()
            return True
        except Exception as e:
            logger.error(f"Scheduled backup failed: {e}")
            return False
            
    return False
