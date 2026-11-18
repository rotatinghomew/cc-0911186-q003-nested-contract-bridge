from __future__ import annotations

from .contract import ALL_KNOWN_FIELDS, METRIC_FIELDS, MISSING, NOT_APPLICABLE, NULL, PRESENT, field_presence as contract_field_presence

# The DWD exposes the two columns that older consumers already read.  Their
# names and meaning must not change: ``contact`` is the single contact value
# and ``source`` is the acquisition channel, defaulting to ``unknown``.
DWD_LEGACY_COLUMNS = ("business_key", "contact", "source")

# ADS fields carry enough metadata for a consumer to tell real input from a
# derived or never-collected value.  Only fields listed in METRIC_FIELDS may
# feed metrics; the rest are descriptive.
ADS_FIELDS = {
    "contact": {"kind": "legacy_column", "metric": False, "derived_from": "contact_channels"},
    "source": {"kind": "legacy_column", "metric": False, "derived_from": "enrollment_source.channel"},
    "contact_channels": {"kind": "input", "metric": False, "derived_from": None},
    "enrollment_source": {"kind": "input", "metric": False, "derived_from": None},
}


def field_presence(record: dict, field: str) -> str:
    return contract_field_presence(record, field)


def legacy_projection(record: dict) -> dict:
    """The DWD row as old consumers read it (unchanged shape)."""

    return {"contact": record.get("contact"), "source": record.get("source", "unknown")}


def legacy_row(fact: dict) -> dict:
    """The unchanged three-column DWD row.

    ``source`` keeps the historical ``unknown`` default when the input never
    carried a channel.  An explicit null stays null: turning it into
    ``unknown`` would erase what the producer actually sent.
    """

    origins = fact.get("field_origins") or {}
    source = fact.get("source_channel")
    if source is None and origins.get("source") != "input_null":
        source = "unknown"
    return {"business_key": fact["business_key"], "contact": fact.get("contact"), "source": source}


def _effective_presence(fact: dict, name: str) -> str:
    """Was the input backing this exported column absent, null, or supplied?

    ``origin`` explains how the exported value was produced; ``presence``
    answers the narrower question an operator asks about the column: does the
    value come from input that actually existed?  A legacy column in a v2 row
    inherits the state of the nested input it was derived from, and an
    explicitly empty input is reported as null rather than missing.
    """

    presence = dict(fact.get("field_presence") or {})
    origins = dict(fact.get("field_origins") or {})
    origin = origins.get(name)
    value = _field_value(fact, name)

    if origin == NOT_APPLICABLE:
        return NOT_APPLICABLE
    if origin in {None, "not_collected", "missing", "default"}:
        return MISSING
    if origin == "input_null":
        return NULL

    if origin == "derived":
        backing = ADS_FIELDS[name]["derived_from"]
        if backing == "contact_channels":
            state = presence.get("contact_channels", MISSING)
        elif backing == "enrollment_source.channel":
            state = presence.get("enrollment_source", MISSING)
            if state == PRESENT and (fact.get("enrollment_source") or {}).get("channel") is None:
                state = NULL
        else:
            state = MISSING
        if state != PRESENT:
            return state
    return NULL if value is None else PRESENT


def _field_value(fact: dict, name: str):
    if name == "contact_channels":
        return fact.get("contact_channels")
    if name == "enrollment_source":
        return fact.get("enrollment_source")
    if name == "contact":
        return fact.get("contact")
    return legacy_row(fact)["source"]


def ads_row(fact: dict) -> dict:
    origins = dict(fact.get("field_origins") or {})
    fields: dict[str, dict] = {}
    for name, spec in ADS_FIELDS.items():
        value = _field_value(fact, name)
        state = _effective_presence(fact, name)
        fields[name] = {
            "value": value,
            # ``presence`` describes the input that backs this column: was it
            # absent, explicitly null, or supplied?  ``available`` answers the
            # different question of whether this export carries a usable
            # value.  Keeping them apart avoids reading a deliberate empty
            # input as "nothing was collected".
            "presence": state,
            # A column is available only when the export carries a value that
            # was actually backed by input, not a "unknown" fallback.
            "available": value is not None and state in {PRESENT, NOT_APPLICABLE},
            "origin": origins.get(name, "not_collected"),
            "kind": spec["kind"],
            "metric": spec["metric"],
            "derived_from": spec["derived_from"],
        }
    return {
        "business_key": fact.get("business_key"),
        "value": fact.get("value"),
        "occurred_at": fact.get("occurred_at"),
        "schema_version": fact.get("schema_version"),
        "fields": fields,
        "legacy_columns": legacy_row(fact),
        "provenance": {
            "source": fact.get("source"),
            "batch_id": fact.get("source_batch"),
            "cursor": fact.get("source_cursor"),
            "fact_version": fact.get("version"),
            "updated_at": fact.get("updated_at"),
        },
    }


def ads_excluded(row: dict) -> dict:
    """A quarantined record, explicitly marked as not exported as a fact."""

    return {
        "source": row.get("source"),
        "batch_id": row.get("batch_id"),
        "cursor": row.get("cursor"),
        "record_index": row.get("record_index"),
        "schema_version": row.get("schema_version"),
        "error_code": row.get("error_code"),
        "error_message": row.get("error_message"),
        "details": row.get("details"),
        "raw": row.get("raw"),
        "exported_as_fact": False,
        "replayed_at": row.get("replayed_at"),
    }


def ads_export(fact_rows: list[dict], excluded_rows: list[dict] | None = None) -> dict:
    rows = [ads_row(fact) for fact in fact_rows]
    excluded = [ads_excluded(row) for row in (excluded_rows or [])]
    return {
        "schema_version": "ads-v2",
        "columns": {name: dict(spec) for name, spec in ADS_FIELDS.items()},
        "legacy_columns": list(DWD_LEGACY_COLUMNS),
        "metric_fields": sorted(METRIC_FIELDS),
        "known_fields": sorted(ALL_KNOWN_FIELDS),
        "presence_values": [PRESENT, NULL, MISSING, NOT_APPLICABLE],
        "row_count": len(rows),
        "excluded_count": len(excluded),
        "rows": rows,
        "excluded": excluded,
    }
