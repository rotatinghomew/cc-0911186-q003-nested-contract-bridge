from __future__ import annotations

import argparse
import json

from .derive import apply_events, fact, facts
from .ingest import progress, quarantined, raw_batch, receive, replay_quarantine
from .projection import ads_export
from .publish import publish, read_manifest, verify_manifest
from .storage import open_store


def _print(value) -> None:
    print(json.dumps(value, ensure_ascii=False))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="本地分层数据服务")
    parser.add_argument("database")
    parser.add_argument(
        "action",
        choices=["receive", "raw", "facts", "fact", "progress", "quarantine", "replay", "publish", "read", "verify", "export"],
    )
    parser.add_argument("payload", nargs="?")
    args = parser.parse_args(argv)
    connection = open_store(args.database)
    try:
        if args.action == "receive":
            data = json.loads(args.payload or "{}")
            result = receive(connection, data["source"], data["batch_id"], data["cursor"], data["records"])
            _print(
                {
                    "received": len(result.events),
                    "rejected": len(result.rejected),
                    "duplicate": result.duplicate,
                    "changed": apply_events(connection, result.events),
                    "errors": result.rejected,
                }
            )
        elif args.action == "raw":
            data = json.loads(args.payload or "{}")
            row = raw_batch(connection, data["source"], data["batch_id"])
            if row is None:
                raise KeyError("批次不存在")
            _print(row)
        elif args.action == "facts":
            _print(facts(connection))
        elif args.action == "fact":
            data = json.loads(args.payload or "{}")
            row = fact(connection, data["source"], data["business_key"])
            if row is None:
                raise KeyError("事实不存在")
            _print(row)
        elif args.action == "progress":
            _print(progress(connection))
        elif args.action == "quarantine":
            data = json.loads(args.payload or "{}")
            _print(
                quarantined(
                    connection,
                    source=data.get("source"),
                    batch_id=data.get("batch_id"),
                    include_replayed=bool(data.get("include_replayed", False)),
                )
            )
        elif args.action == "replay":
            data = json.loads(args.payload or "{}")
            result = replay_quarantine(connection, source=data.get("source"), batch_id=data.get("batch_id"))
            changed = apply_events(connection, result["accepted"]) if result["accepted"] else 0
            _print({"examined": result["examined"], "accepted": len(result["accepted"]), "still_rejected": result["still_rejected"], "changed": changed})
        elif args.action == "publish":
            data = json.loads(args.payload or "{}")
            payload = publish(connection, data["name"], data["version"])
            _print({"name": payload["name"], "version": payload["version"], "row_count": payload["row_count"], "excluded_count": payload["excluded_count"]})
        elif args.action == "read":
            data = json.loads(args.payload or "{}")
            _print(read_manifest(connection, data["name"], data["version"]))
        elif args.action == "verify":
            data = json.loads(args.payload or "{}")
            _print(verify_manifest(connection, data["name"], data["version"]))
        else:
            data = json.loads(args.payload or "{}")
            _print(ads_export(facts(connection), quarantined(connection)))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
