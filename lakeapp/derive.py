from __future__ import annotations

from .contract import SCHEMA_V1, Event
from .storage import decode, encode, transaction


def _payload(event: Event) -> str:
    payload = {"value": event.value, "occurred_at": event.occurred_at}
    # Keep the payload of an old-shaped row identical to the baseline, so
    # anything that already reads it sees no new keys.  A v2 row records the
    # new fields even when null, because the null itself is the input.
    if event.schema_version != SCHEMA_V1:
        payload["contact_channels"] = list(event.contact_channels) if event.contact_channels is not None else None
        payload["enrollment_source"] = event.enrollment_source
    return encode(payload)


def apply_events(connection, events: list[Event]) -> int:
    changed = 0
    with transaction(connection):
        for event in events:
            if event.kind == "retract":
                connection.execute("DELETE FROM facts WHERE source=? AND business_key=?", (event.source, event.business_key))
            else:
                connection.execute(
                    "INSERT INTO facts(source,business_key,payload,source_batch,version,kind,schema_version,contact,source_channel,"
                    "contact_channels,enrollment_source,field_presence,field_origins,source_cursor,updated_at) "
                    "VALUES(?,?,?,?,1,?,?,?,?,?,?,?,?,?,datetime('now')) "
                    "ON CONFLICT(source,business_key) DO UPDATE SET payload=excluded.payload,source_batch=excluded.source_batch,"
                    "version=facts.version+1,kind=excluded.kind,schema_version=excluded.schema_version,contact=excluded.contact,"
                    "source_channel=excluded.source_channel,contact_channels=excluded.contact_channels,"
                    "enrollment_source=excluded.enrollment_source,field_presence=excluded.field_presence,"
                    "field_origins=excluded.field_origins,source_cursor=excluded.source_cursor,updated_at=excluded.updated_at",
                    (
                        event.source,
                        event.business_key,
                        _payload(event),
                        event.batch_id,
                        event.kind,
                        event.schema_version,
                        event.contact,
                        event.legacy_source,
                        encode(list(event.contact_channels)) if event.contact_channels is not None else None,
                        encode(event.enrollment_source) if event.enrollment_source is not None else None,
                        encode(event.presence),
                        encode(event.origins),
                        event.cursor,
                    ),
                )
            changed += 1
    return changed


def _fact(row) -> dict:
    payload = decode(row["payload"])
    return {
        "source": row["source"],
        "business_key": row["business_key"],
        "payload": payload,
        "value": payload.get("value"),
        "occurred_at": payload.get("occurred_at"),
        "source_batch": row["source_batch"],
        "version": row["version"],
        "kind": row["kind"],
        "schema_version": row["schema_version"],
        "contact": row["contact"],
        "source_channel": row["source_channel"],
        "contact_channels": decode(row["contact_channels"]) if row["contact_channels"] is not None else None,
        "enrollment_source": decode(row["enrollment_source"]) if row["enrollment_source"] is not None else None,
        "field_presence": decode(row["field_presence"]) if row["field_presence"] is not None else {},
        "field_origins": decode(row["field_origins"]) if row["field_origins"] is not None else {},
        "source_cursor": row["source_cursor"],
        "updated_at": row["updated_at"],
    }


FACT_COLUMNS = (
    "source,business_key,payload,source_batch,version,kind,schema_version,contact,source_channel,"
    "contact_channels,enrollment_source,field_presence,field_origins,source_cursor,updated_at"
)


def facts(connection) -> list[dict]:
    return [_fact(row) for row in connection.execute(f"SELECT {FACT_COLUMNS} FROM facts ORDER BY source,business_key")]


def fact(connection, source: str, business_key: str) -> dict | None:
    row = connection.execute(
        f"SELECT {FACT_COLUMNS} FROM facts WHERE source=? AND business_key=?", (source, business_key)
    ).fetchone()
    return _fact(row) if row else None
