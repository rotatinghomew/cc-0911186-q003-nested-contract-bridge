import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lakeapp.contract import (
    METRIC_FIELDS,
    ContractError,
    detect_schema_version,
    field_presence,
    parse_event,
)
from lakeapp.derive import apply_events, facts
from lakeapp.ingest import quarantined, raw_batch, receive, replay_quarantine
from lakeapp.projection import ads_export, ads_row, legacy_row
from lakeapp.publish import publish, read_manifest, verify_manifest
from lakeapp.storage import open_store


def v1_record(**overrides):
    record = {
        "business_key": "member-1",
        "kind": "upsert",
        "value": 3,
        "occurred_at": "2026-11-01T09:00:00",
        "contact": "old@example.com",
        "source": "web",
    }
    record.update(overrides)
    return record


def v2_record(**overrides):
    record = {
        "schema_version": "v2",
        "business_key": "member-2",
        "kind": "upsert",
        "value": 5,
        "occurred_at": "2026-11-02T09:00:00",
        "contact_channels": [{"type": "email", "value": "new@example.com"}],
        "enrollment_source": {"channel": "app", "campaign": "autumn", "medium": "push"},
    }
    record.update(overrides)
    return record


class ContractTest(unittest.TestCase):
    def test_version_is_detected_and_old_records_remain_valid(self):
        self.assertEqual(detect_schema_version(v1_record()), "v1")
        self.assertEqual(detect_schema_version(v2_record()), "v2")
        self.assertEqual(detect_schema_version(v2_record(schema_version="v1")), "v1")

    def test_unknown_schema_version_is_rejected_not_guessed(self):
        with self.assertRaises(ContractError) as raised:
            parse_event("enrollment", "b1", 1, v2_record(schema_version="v3"))
        self.assertEqual(raised.exception.code, "unsupported_schema_version")

    def test_unknown_field_is_rejected_without_being_flattened(self):
        with self.assertRaises(ContractError) as raised:
            parse_event("enrollment", "b1", 1, v1_record(preferred_language="zh"))
        self.assertEqual(raised.exception.code, "unknown_field")
        self.assertEqual(raised.exception.details["fields"], ["preferred_language"])

    def test_field_from_other_version_is_not_silently_accepted(self):
        with self.assertRaises(ContractError) as raised:
            parse_event("enrollment", "b1", 1, v1_record(contact_channels=[]))
        self.assertEqual(raised.exception.code, "field_not_allowed")

    def test_missing_and_explicit_null_are_different(self):
        self.assertEqual(field_presence({}, "contact_channels"), "missing")
        self.assertEqual(field_presence({"contact_channels": None}, "contact_channels"), "null")
        self.assertEqual(field_presence({"contact_channels": []}, "contact_channels"), "present")

        explicit = parse_event("enrollment", "b1", 1, v2_record(schema_version="v2", contact_channels=None))
        self.assertEqual(explicit.presence["contact_channels"], "null")
        self.assertEqual(explicit.origins["contact_channels"], "input_null")

        absent = parse_event("enrollment", "b1", 1, {"schema_version": "v2", "business_key": "k", "kind": "upsert",
                                                     "value": 1, "occurred_at": "t"})
        self.assertEqual(absent.presence["contact_channels"], "missing")
        self.assertEqual(absent.origins["contact_channels"], "not_collected")

    def test_unknown_nested_field_is_rejected(self):
        record = v2_record(contact_channels=[{"type": "email", "value": "a@b", "verified": True}])
        with self.assertRaises(ContractError) as raised:
            parse_event("enrollment", "b1", 1, record)
        self.assertEqual(raised.exception.code, "unknown_field")
        self.assertIn("contact_channels[0].verified", raised.exception.details["fields"])

    def test_metric_fields_are_an_explicit_allow_list(self):
        self.assertEqual(set(METRIC_FIELDS), {"value"})


class IngestionTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.connection = open_store(Path(self.tempdir.name) / "data.db")

    def tearDown(self):
        self.connection.close()
        self.tempdir.cleanup()

    def test_bad_record_does_not_reject_the_whole_batch(self):
        result = receive(
            self.connection,
            "enrollment",
            "batch-1",
            1,
            [v1_record(), v2_record(), v2_record(schema_version="v9", business_key="bad")],
        )
        self.assertEqual([event.business_key for event in result.events], ["member-1", "member-2"])
        self.assertEqual(len(result.rejected), 1)
        self.assertEqual(result.rejected[0]["code"], "unsupported_schema_version")
        apply_events(self.connection, result.events)
        self.assertEqual(len(facts(self.connection)), 2)

    def test_raw_batch_keeps_input_version_and_digest(self):
        records = [v1_record(), v2_record()]
        receive(self.connection, "enrollment", "batch-1", 1, records)
        stored = raw_batch(self.connection, "enrollment", "batch-1")
        self.assertEqual(stored["records"], records)
        self.assertEqual(stored["record_count"], 2)
        payload = json.dumps(
            {"cursor": 1, "records": records}, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        self.assertEqual(stored["payload_sha256"], hashlib.sha256(payload.encode("utf-8")).hexdigest())
        # ODS is version agnostic: both inputs are preserved verbatim.
        self.assertEqual(stored["records"][0]["contact"], "old@example.com")
        self.assertEqual(stored["records"][1]["contact_channels"][0]["value"], "new@example.com")

    def test_quarantine_records_are_queryable_with_original_payload(self):
        raw = v2_record(business_key="bad", contact_channels=[{"type": "email", "value": "a@b"},
                                                              {"type": "phone", "value": "1"}])
        receive(self.connection, "enrollment", "batch-1", 1, [raw])
        entries = quarantined(self.connection)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["error_code"], "ambiguous_legacy_projection")
        self.assertEqual(entries[0]["raw"], raw)
        self.assertEqual(entries[0]["schema_version"], "v2")
        self.assertIsNone(entries[0]["replayed_at"])

    def test_replay_records_attempts_and_leaves_rejected_records_visible(self):
        receive(self.connection, "enrollment", "batch-1", 1, [v1_record(mystery=1)])
        result = replay_quarantine(self.connection)
        self.assertEqual(result["still_rejected"], 1)
        self.assertEqual(result["accepted"], [])
        entry = quarantined(self.connection)[0]
        self.assertEqual(entry["replay_attempts"], 1)
        self.assertIsNotNone(entry["last_replay_error"])

    def test_replay_of_a_now_valid_record_produces_a_fact(self):
        # Simulate a record that an earlier contract could not parse.
        receive(self.connection, "enrollment", "batch-1", 1, [v1_record(mystery=1)])
        self.connection.execute("UPDATE quarantine SET raw=? WHERE id=1", (json.dumps(v2_record()),))
        self.connection.commit()
        result = replay_quarantine(self.connection)
        self.assertEqual(len(result["accepted"]), 1)
        apply_events(self.connection, result["accepted"])
        self.assertEqual([row["business_key"] for row in facts(self.connection)], ["member-2"])
        self.assertIsNotNone(quarantined(self.connection, include_replayed=True)[0]["replayed_at"])

    def test_duplicate_delivery_does_not_duplicate_quarantine_rows(self):
        record = v1_record(mystery=1)
        receive(self.connection, "enrollment", "batch-1", 1, [record])
        replay_quarantine(self.connection)
        receive(self.connection, "enrollment", "batch-1", 1, [record])
        entries = quarantined(self.connection)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["replay_attempts"], 1)

    def test_replayed_batch_is_idempotent_and_does_not_rewrite_raw(self):
        original = [v1_record(mystery=1)]
        receive(self.connection, "enrollment", "batch-1", 1, original)
        receive(self.connection, "enrollment", "batch-1", 1, original)
        self.assertEqual(len(quarantined(self.connection)), 1)
        with self.assertRaises(ValueError):
            receive(self.connection, "enrollment", "batch-1", 1, [v1_record()])


class ProjectionTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.connection = open_store(Path(self.tempdir.name) / "data.db")
        result = receive(self.connection, "enrollment", "batch-1", 1, [v1_record(), v2_record()])
        apply_events(self.connection, result.events)
        self.rows = {row["business_key"]: row for row in facts(self.connection)}

    def tearDown(self):
        self.connection.close()
        self.tempdir.cleanup()

    def test_dwd_keeps_legacy_columns_and_values(self):
        old = legacy_row(self.rows["member-1"])
        self.assertEqual(set(old), {"business_key", "contact", "source"})
        self.assertEqual(old["contact"], "old@example.com")
        self.assertEqual(old["source"], "web")

        new = legacy_row(self.rows["member-2"])
        self.assertEqual(set(new), {"business_key", "contact", "source"})
        self.assertEqual(new["contact"], "new@example.com")
        self.assertEqual(new["source"], "app")

    def test_dwd_defaults_source_only_when_input_never_had_one(self):
        no_source = parse_event("enrollment", "b1", 1, {"business_key": "k", "kind": "upsert", "value": 1, "occurred_at": "t"})
        apply_events(self.connection, [no_source])
        row = [fact for fact in facts(self.connection) if fact["business_key"] == "k"][0]
        self.assertEqual(legacy_row(row)["source"], "unknown")

        explicit_null = parse_event("enrollment", "b1", 1, v2_record(business_key="k2", enrollment_source=None))
        apply_events(self.connection, [explicit_null])
        row = [fact for fact in facts(self.connection) if fact["business_key"] == "k2"][0]
        self.assertIsNone(legacy_row(row)["source"])

    def test_v1_payload_stays_byte_compatible_with_the_baseline(self):
        self.assertEqual(self.rows["member-1"]["payload"],
                         {"value": 3.0, "occurred_at": "2026-11-01T09:00:00"})
        self.assertIn("contact_channels", self.rows["member-2"]["payload"])

    def test_ads_exposes_new_fields_with_presence_and_origin(self):
        ads = ads_export([self.rows["member-2"]])
        fields = ads["rows"][0]["fields"]
        self.assertEqual(ads["rows"][0]["schema_version"], "v2")
        self.assertEqual(fields["contact_channels"]["value"], [{"type": "email", "value": "new@example.com"}])
        self.assertEqual(fields["contact_channels"]["presence"], "present")
        self.assertEqual(fields["contact_channels"]["origin"], "input")
        self.assertEqual(fields["enrollment_source"]["value"]["campaign"], "autumn")
        self.assertEqual(fields["contact"]["origin"], "derived")
        self.assertEqual(fields["contact"]["derived_from"], "contact_channels")
        self.assertEqual(ads["legacy_columns"], ["business_key", "contact", "source"])

    def test_ads_distinguishes_missing_from_explicit_null(self):
        records = [
            v2_record(business_key="nulls", contact_channels=None, enrollment_source=None),
            v2_record(business_key="absent"),
        ]
        records[1].pop("contact_channels")
        records[1].pop("enrollment_source")
        apply_events(self.connection, receive(self.connection, "enrollment", "batch-2", 2, records).events)
        rows = {row["business_key"]: row for row in facts(self.connection)}
        explicit = ads_row(rows["nulls"])["fields"]
        absent = ads_row(rows["absent"])["fields"]
        self.assertEqual(explicit["contact_channels"]["presence"], "null")
        self.assertEqual(explicit["contact_channels"]["origin"], "input_null")
        self.assertEqual(absent["contact_channels"]["presence"], "missing")
        self.assertEqual(absent["contact_channels"]["origin"], "not_collected")
        self.assertFalse(absent["contact_channels"]["available"])

    def test_v1_rows_do_not_claim_new_fields_were_collected(self):
        fields = ads_export([self.rows["member-1"]])["rows"][0]["fields"]
        self.assertEqual(fields["contact_channels"]["presence"], "not_applicable")
        self.assertFalse(fields["contact_channels"]["available"])
        self.assertEqual(fields["contact"]["presence"], "present")

    def test_only_allow_listed_fields_can_reach_metrics(self):
        ads = ads_export(list(self.rows.values()))
        self.assertEqual(ads["metric_fields"], ["value"])
        self.assertTrue(all(spec["metric"] is False for spec in ads["columns"].values()))
        result = receive(self.connection, "enrollment", "batch-9", 9,
                         [v1_record(business_key="x", conversion_score=1)])
        self.assertEqual(result.events, [])
        self.assertEqual(result.rejected[0]["code"], "unknown_field")
        exported = ads_export(facts(self.connection), quarantined(self.connection))
        self.assertNotIn("conversion_score", json.dumps(exported["rows"]))


class PublishTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.connection = open_store(Path(self.tempdir.name) / "data.db")
        result = receive(
            self.connection,
            "enrollment",
            "batch-1",
            1,
            [v1_record(), v2_record(), v2_record(business_key="bad", contact_channels=[
                {"type": "email", "value": "a@b"}, {"type": "phone", "value": "1"}])],
        )
        apply_events(self.connection, result.events)

    def tearDown(self):
        self.connection.close()
        self.tempdir.cleanup()

    def test_manifest_keeps_legacy_rows_and_adds_ads_and_provenance(self):
        publish(self.connection, "daily", 1)
        manifest = read_manifest(self.connection, "daily", 1)
        self.assertEqual(manifest["row_count"], 2)
        for row in manifest["rows"]:
            self.assertEqual(set(row), {"business_key", "contact", "source"})
        ads_rows = {row["business_key"]: row for row in manifest["ads"]["rows"]}
        self.assertEqual(ads_rows["member-2"]["fields"]["enrollment_source"]["value"]["channel"], "app")
        self.assertEqual(ads_rows["member-2"]["provenance"]["batch_id"], "batch-1")
        self.assertEqual(ads_rows["member-2"]["provenance"]["cursor"], 1)
        self.assertEqual(manifest["excluded_count"], 1)
        self.assertEqual(manifest["ads"]["excluded"][0]["exported_as_fact"], False)
        self.assertTrue(all(verify_manifest(self.connection, "daily", 1).values()))

    def test_tampering_with_a_manifest_is_detected(self):
        publish(self.connection, "daily", 1)
        manifest = read_manifest(self.connection, "daily", 1)
        manifest["rows"][0]["contact"] = "tampered@example.com"
        self.connection.execute(
            "UPDATE manifests SET payload=? WHERE name='daily' AND version=1",
            (json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")),),
        )
        self.connection.commit()
        self.assertFalse(verify_manifest(self.connection, "daily", 1)["dwd_ok"])

    def test_republishing_a_version_replaces_it(self):
        publish(self.connection, "daily", 1)
        publish(self.connection, "daily", 1)
        self.assertEqual(read_manifest(self.connection, "daily", 1)["row_count"], 2)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM manifests").fetchone()[0], 1)


class StoreCompatibilityTest(unittest.TestCase):
    def test_existing_store_gains_new_tables_and_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "old.db"
            legacy = sqlite3.connect(path)
            legacy.executescript(
                """
                CREATE TABLE facts (source TEXT NOT NULL, business_key TEXT NOT NULL, payload TEXT NOT NULL,
                                    source_batch TEXT NOT NULL, version INTEGER NOT NULL,
                                    PRIMARY KEY (source, business_key));
                INSERT INTO facts VALUES ('enrollment','member-1','{"occurred_at":"t","value":1}','batch-1',1);
                """
            )
            legacy.commit()
            legacy.close()

            connection = open_store(path)
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(facts)")}
            for name in ("contact", "source_channel", "schema_version", "field_presence", "source_cursor"):
                self.assertIn(name, columns)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM raw_batches").fetchone()[0], 0)
            rows = facts(connection)
            self.assertEqual(rows[0]["business_key"], "member-1")
            self.assertEqual(rows[0]["value"], 1)
            connection.close()


if __name__ == "__main__":
    unittest.main()
