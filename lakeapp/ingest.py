from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from .contract import ContractError, Event, detect_schema_version, parse_event
from .storage import decode, encode, transaction


@dataclass
class BatchResult:
    """Outcome of receiving one batch.

    Accepted records are parsed events; rejected records are reported and
    stored in quarantine with their original payload.  The batch itself is
    persisted exactly once, byte for byte, so a replay can rebuild the same
    decisions later.
    """

    events: list[Event] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)
    duplicate: bool = False

    def __iter__(self):
        return iter(self.events)

    def __len__(self) -> int:
        return len(self.events)

    def __getitem__(self, index):
        return self.events[index]


def _quarantine(connection, source: str, batch_id: str, cursor: int, index: int, raw: dict, version: str | None, exc: ContractError) -> dict:
    connection.execute(
        # A record keeps the first rejection it produced.  A duplicate batch
        # must not resurrect a record that a replay already accepted, and the
        # stored error can only change through replay, which owns the status.
        "INSERT INTO quarantine(source,batch_id,cursor,record_index,schema_version,error_code,error_message,details,raw,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,datetime('now')) "
        "ON CONFLICT(source,batch_id,record_index) DO NOTHING",
        (source, batch_id, cursor, index, version, exc.code, str(exc), encode(exc.details), encode(raw)),
    )
    return {
        "index": index,
        "code": exc.code,
        "message": str(exc),
        "details": exc.details,
        "schema_version": version,
    }


def _parse_records(source: str, batch_id: str, cursor: int, records: list[dict]) -> tuple[list[Event], list[dict]]:
    events: list[Event] = []
    rejected: list[dict] = []
    for index, raw in enumerate(records):
        try:
            events.append(parse_event(source, batch_id, cursor, raw))
        except ContractError as exc:
            version = None
            if isinstance(raw, dict):
                try:
                    version = detect_schema_version(raw)
                except ContractError:
                    declared = raw.get("schema_version")
                    version = declared if isinstance(declared, str) else None
            rejected.append({"raw": raw, "index": index, "version": version, "error": exc})
    return events, rejected


def receive(connection, source: str, batch_id: str, cursor: int, records: list[dict]) -> BatchResult:
    existing = connection.execute(
        "SELECT payload FROM raw_batches WHERE source=? AND batch_id=?", (source, batch_id)
    ).fetchone()
    payload = {"cursor": cursor, "records": records}
    if existing and existing["payload"] != encode(payload):
        raise ValueError("同一批次内容冲突")
    if not existing:
        with transaction(connection):
            connection.execute(
                "INSERT INTO raw_batches(source,batch_id,cursor,payload,received_at) VALUES(?,?,?,?,datetime('now'))",
                (source, batch_id, cursor, encode(payload)),
            )
            connection.execute(
                "INSERT INTO source_progress(source,cursor) VALUES(?,?) ON CONFLICT(source) DO UPDATE SET cursor=MAX(cursor,excluded.cursor)",
                (source, cursor),
            )
    events, rejected = _parse_records(source, batch_id, cursor, records)

    result = BatchResult(events=events, duplicate=bool(existing))
    if rejected or existing:
        with transaction(connection):
            for item in rejected:
                result.rejected.append(
                    _quarantine(
                        connection,
                        source,
                        batch_id,
                        cursor,
                        item["index"],
                        item["raw"],
                        item["version"],
                        item["error"],
                    )
                )
            if existing:
                # The same batch was re-delivered and now parses; close any
                # rejection the previous parser left open for these records.
                for index in range(len(records)):
                    if index in {item["index"] for item in rejected}:
                        continue
                    connection.execute(
                        "UPDATE quarantine SET replayed_at=datetime('now'),last_replay_error=NULL "
                        "WHERE source=? AND batch_id=? AND record_index=? AND replayed_at IS NULL",
                        (source, batch_id, index),
                    )
    return result


def quarantined(connection, *, source: str | None = None, batch_id: str | None = None, include_replayed: bool = False) -> list[dict]:
    clauses: list[str] = []
    parameters: list[object] = []
    if source is not None:
        clauses.append("source=?")
        parameters.append(source)
    if batch_id is not None:
        clauses.append("batch_id=?")
        parameters.append(batch_id)
    if not include_replayed:
        clauses.append("replayed_at IS NULL")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = connection.execute(
        "SELECT id,source,batch_id,cursor,record_index,schema_version,error_code,error_message,details,raw,created_at,replayed_at,replay_attempts,last_replay_error "
        "FROM quarantine" + where + " ORDER BY id",
        parameters,
    )
    result = []
    for row in rows:
        item = dict(row)
        item["details"] = decode(item["details"])
        item["raw"] = decode(item["raw"])
        result.append(item)
    return result


def replay_quarantine(connection, *, source: str | None = None, batch_id: str | None = None) -> dict:
    """Re-run the current parser over stored rejected records.

    The raw payload stays untouched; only the quarantine status changes.  A
    record that is still unacceptable remains queryable with an updated
    attempt count and error.
    """

    entries = quarantined(connection, source=source, batch_id=batch_id, include_replayed=False)
    accepted: list[Event] = []
    still_rejected = 0
    with transaction(connection):
        for entry in entries:
            entry_source = entry["source"]
            entry_batch = entry["batch_id"]
            index = entry["record_index"]
            try:
                event = parse_event(entry_source, entry_batch, entry["cursor"], entry["raw"])
            except ContractError as exc:
                still_rejected += 1
                connection.execute(
                    "UPDATE quarantine SET replay_attempts=replay_attempts+1,last_replay_error=?,error_code=?,error_message=?,details=? "
                    "WHERE source=? AND batch_id=? AND record_index=? AND replayed_at IS NULL",
                    (str(exc), exc.code, str(exc), encode(exc.details), entry_source, entry_batch, index),
                )
                continue
            accepted.append(event)
            connection.execute(
                "UPDATE quarantine SET replayed_at=datetime('now'),replay_attempts=replay_attempts+1,last_replay_error=NULL "
                "WHERE source=? AND batch_id=? AND record_index=? AND replayed_at IS NULL",
                (entry_source, entry_batch, index),
            )
    return {"accepted": accepted, "still_rejected": still_rejected, "examined": len(entries)}


def raw_batch(connection, source: str, batch_id: str) -> dict | None:
    """The ODS record exactly as received, for replay and audit."""

    row = connection.execute(
        "SELECT payload,received_at FROM raw_batches WHERE source=? AND batch_id=?", (source, batch_id)
    ).fetchone()
    if not row:
        return None
    payload = decode(row["payload"])
    return {
        "source": source,
        "batch_id": batch_id,
        "cursor": payload["cursor"],
        "records": payload["records"],
        "record_count": len(payload["records"]),
        "payload_sha256": hashlib.sha256(row["payload"].encode("utf-8")).hexdigest(),
        "received_at": row["received_at"],
    }


def progress(connection) -> list[dict]:
    return [dict(row) for row in connection.execute("SELECT source,cursor FROM source_progress ORDER BY source")]
