from __future__ import annotations

import hashlib

from .contract import METRIC_FIELDS
from .derive import facts
from .ingest import quarantined
from .projection import DWD_LEGACY_COLUMNS, ads_export, legacy_row
from .storage import decode, encode, transaction


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _build(name: str, version: int, fact_rows: list[dict], excluded: list[dict]) -> dict:
    dwd_rows = [legacy_row(fact) for fact in fact_rows]
    dwd_payload = encode(dwd_rows)
    ads = ads_export(fact_rows, excluded)
    ads_payload = encode(ads)
    return {
        "name": name,
        "version": version,
        "rows": dwd_rows,
        "row_count": len(dwd_rows),
        "dwd": {
            "columns": list(DWD_LEGACY_COLUMNS),
            "row_count": len(dwd_rows),
            "sha256": _digest(dwd_payload),
        },
        "ads": ads,
        "ads_sha256": _digest(ads_payload),
        "metric_fields": sorted(METRIC_FIELDS),
        "excluded_count": len(excluded),
        "excluded_sha256": _digest(encode([item["raw"] for item in excluded])),
    }


def _coerce_rows(connection, rows: list[dict] | None) -> list[dict]:
    """Accept either stored fact rows or a bare list from older callers."""

    if rows is None:
        return facts(connection)
    if not rows:
        return []
    if "schema_version" in rows[0] and "field_presence" in rows[0]:
        return rows
    # A caller handed over a legacy-shaped snapshot; publish the facts that
    # actually back it so provenance and the ADS are never guessed.
    return facts(connection)


def publish(connection, name: str, version: int, rows: list[dict] | None = None) -> dict:
    """Freeze one version.

    ``rows`` is accepted for compatibility with older callers but the DWD rows
    are rebuilt from the stored facts.  Tests can still pass ``[]`` to publish
    an empty daily snapshot.
    """

    fact_rows = _coerce_rows(connection, rows)
    excluded = quarantined(connection)
    payload = _build(name, version, fact_rows, excluded)
    with transaction(connection):
        connection.execute(
            "INSERT INTO manifests(name,version,payload,status) VALUES(?,?,?,?) "
            "ON CONFLICT(name,version) DO UPDATE SET payload=excluded.payload,status=excluded.status",
            (name, version, encode(payload), "ready"),
        )
    return payload


def read_manifest(connection, name: str, version: int) -> dict:
    row = connection.execute("SELECT payload,status FROM manifests WHERE name=? AND version=?", (name, version)).fetchone()
    if not row or row["status"] != "ready":
        raise KeyError("版本不可读")
    return decode(row["payload"])


def verify_manifest(connection, name: str, version: int) -> dict:
    payload = read_manifest(connection, name, version)
    dwd_payload = encode(payload["rows"])
    ads_payload = encode(payload["ads"])
    return {
        "dwd_ok": _digest(dwd_payload) == payload["dwd"]["sha256"],
        "ads_ok": _digest(ads_payload) == payload["ads_sha256"],
        "row_count_ok": len(payload["rows"]) == payload["row_count"] == payload["dwd"]["row_count"],
        "excluded_ok": _digest(encode([item["raw"] for item in payload["ads"]["excluded"]]))
        == payload["excluded_sha256"]
        and len(payload["ads"]["excluded"]) == payload["excluded_count"],
    }
