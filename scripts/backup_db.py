#!/usr/bin/env python3
"""Database backup script with rotation.

Usage:
    python scripts/backup_db.py                    # Run backup now
    python scripts/backup_db.py --schedule         # Show cron schedule suggestion
    python scripts/backup_db.py --list             # List existing backups
    python scripts/backup_db.py --restore <file>   # Restore from backup

Can be scheduled via cron:
    0 2 * * * cd /path/to/prep-brain && python scripts/backup_db.py >> logs/backup.log 2>&1
"""
import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services import backup

def main() -> None:
    parser = argparse.ArgumentParser(description="Database backup utility")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Show cron schedule suggestion",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List existing backups",
    )
    parser.add_argument(
        "--restore",
        type=str,
        metavar="FILE",
        help="Restore from backup file",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Don't compress the backup",
    )
    parser.add_argument(
        "--max-backups",
        type=int,
        help="Maximum backups to keep",
    )

    args = parser.parse_args()

    if args.schedule:
        print("Add this to your crontab (crontab -e):")
        print()
        print("# Daily database backup at 2 AM")
        print(f"0 2 * * * cd {Path.cwd()} && python scripts/backup_db.py >> logs/backup.log 2>&1")
        return

    if args.list:
        backups = backup.list_backups()
        if not backups:
            print("No backups found.")
            return
        print(f"Found {len(backups)} backup(s):")
        print()
        for b in backups:
            print(f"  {b['name']} ({b['size_mb']:.2f} MB) - {b['created']}")
        return

    if args.restore:
        backup_path = Path(args.restore)
        if not backup_path.is_absolute():
            backup_path = backup.get_backup_dir() / backup_path
        backup.restore_backup(backup_path)
        return

    # Default: create backup and rotate
    backup.create_backup(compress=not args.no_compress)
    backup.rotate_backups(max_backups=args.max_backups)


if __name__ == "__main__":
    main()
