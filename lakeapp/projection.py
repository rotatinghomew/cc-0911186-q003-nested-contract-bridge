from __future__ import annotations


def field_presence(record: dict, field: str) -> str:
    return "present" if field in record else "missing"


def legacy_projection(record: dict) -> dict:
    return {"contact": record.get("contact"), "source": record.get("source", "unknown")}
