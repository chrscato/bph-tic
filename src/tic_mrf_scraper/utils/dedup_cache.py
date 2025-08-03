"""Disk-backed deduplication cache using SQLite."""

from pathlib import Path
import sqlite3
import tempfile
from typing import Optional


class SQLiteDedupCache:
    """A disk-backed set-like cache for deduplication.

    Stores seen values in a SQLite database to avoid unbounded
    in-memory growth. Supports membership checks, adding new
    values, and resetting the cache.
    """

    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            self._temp_file = tempfile.NamedTemporaryFile(delete=False)
            db_path = Path(self._temp_file.name)
        else:
            self._temp_file = None
            db_path = Path(db_path)
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("CREATE TABLE IF NOT EXISTS seen (value TEXT PRIMARY KEY)")
        self._conn.commit()

    def add(self, value: str) -> bool:
        """Add a value to the cache.

        Returns True if the value was added, False if it was already present.
        """
        try:
            self._conn.execute("INSERT INTO seen(value) VALUES (?)", (value,))
            self._conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def __contains__(self, value: str) -> bool:
        cur = self._conn.execute("SELECT 1 FROM seen WHERE value=? LIMIT 1", (value,))
        return cur.fetchone() is not None

    def reset(self) -> None:
        """Remove all values from the cache."""
        self._conn.execute("DELETE FROM seen")
        self._conn.commit()

    def close(self) -> None:
        """Close the database connection and remove temporary file."""
        self._conn.close()
        if hasattr(self, "_temp_file") and self._temp_file is not None:
            Path(self._temp_file.name).unlink(missing_ok=True)
