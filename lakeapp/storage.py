from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_batches (
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (source, batch_id)
);
CREATE TABLE IF NOT EXISTS source_progress (
    source TEXT PRIMARY KEY,
    cursor INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS facts (
    source TEXT NOT NULL,
    business_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    source_batch TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (source, business_key)
);
CREATE TABLE IF NOT EXISTS manifests (
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (name, version)
);
CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    record_index INTEGER NOT NULL,
    schema_version TEXT,
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    details TEXT NOT NULL,
    raw TEXT NOT NULL,
    created_at TEXT NOT NULL,
    replayed_at TEXT,
    replay_attempts INTEGER NOT NULL DEFAULT 0,
    last_replay_error TEXT,
    UNIQUE (source, batch_id, record_index)
);
"""

# Columns added after the baseline schema.  ``open_store`` applies them to
# existing databases so that a local store created by an older checkout keeps
# working instead of losing data when reopened.
FACT_COLUMNS = {
    "kind": "TEXT",
    "schema_version": "TEXT",
    "contact": "TEXT",
    "source_channel": "TEXT",
    "contact_channels": "TEXT",
    "enrollment_source": "TEXT",
    "field_presence": "TEXT",
    "field_origins": "TEXT",
    "source_cursor": "INTEGER",
    "updated_at": "TEXT",
}


def _ensure_columns(connection: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
    for name, declaration in columns.items():
        if name not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")


def open_store(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    _ensure_columns(connection, "facts", FACT_COLUMNS)
    connection.commit()
    return connection


@contextmanager
def transaction(connection: sqlite3.Connection):
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def encode(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode(value: str):
    return json.loads(value)
