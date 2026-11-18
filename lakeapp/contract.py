from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Mapping

SCHEMA_V1 = "v1"
SCHEMA_V2 = "v2"
SUPPORTED_SCHEMA_VERSIONS = (SCHEMA_V1, SCHEMA_V2)

PRESENT = "present"
NULL = "null"
MISSING = "missing"
NOT_APPLICABLE = "not_applicable"
NOT_COLLECTED = "not_collected"

COMMON_FIELDS = frozenset({"business_key", "kind", "value", "occurred_at", "schema_version"})
V1_FIELDS = COMMON_FIELDS | {"contact", "source"}
V2_FIELDS = COMMON_FIELDS | {"contact_channels", "enrollment_source"}
ALL_KNOWN_FIELDS = frozenset(V1_FIELDS | V2_FIELDS)
NESTED_FIELDS = {
    "enrollment_source": frozenset({"channel", "campaign", "medium", "detail"}),
    "contact_channels": frozenset({"type", "value"}),
}

# Only fields that have gone through the input contract may feed metrics.  New
# input fields must be added here deliberately, together with a schema version.
METRIC_FIELDS = frozenset({"value"})

CONTACT_CHANNEL_TYPES = frozenset({"email", "phone", "mobile", "sms", "wechat", "wecom", "other"})

REQUIRED_FIELDS = frozenset({"business_key", "kind", "value", "occurred_at"})


class ContractError(ValueError):
    """A single record cannot be accepted under the current contract.

    ``code`` is stable enough to be stored in the quarantine table and queried
    by operators.  ``details`` carries machine-readable context such as the
    offending field names.
    """

    def __init__(self, message: str, *, code: str = "invalid_record", details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


@dataclass(frozen=True)
class Event:
    source: str
    batch_id: str
    cursor: int
    business_key: str
    kind: str
    value: float
    occurred_at: str
    schema_version: str = SCHEMA_V1
    contact: str | None = None
    legacy_source: str | None = None
    contact_channels: tuple[dict[str, str], ...] | None = None
    enrollment_source: dict[str, str | None] | None = None
    presence: dict[str, str] = field(default_factory=dict)
    origins: dict[str, str] = field(default_factory=dict)


def field_presence(record: Mapping[str, Any], name: str) -> str:
    """Return present/null/missing for an input field.

    A caller must be able to tell ``{"contact_channels": null}`` (the producer
    deliberately cleared the value) from ``{}`` (the value was never sampled).
    """

    if name not in record:
        return MISSING
    return NULL if record[name] is None else PRESENT


def detect_schema_version(raw: Mapping[str, Any]) -> str:
    """Detect the input version, refusing versions the contract does not know."""

    if "schema_version" in raw:
        version = raw["schema_version"]
        if version not in SUPPORTED_SCHEMA_VERSIONS:
            raise ContractError(
                f"不支持的 schema_version: {version!r}",
                code="unsupported_schema_version",
                details={"schema_version": version},
            )
        return version
    if "contact_channels" in raw or "enrollment_source" in raw:
        return SCHEMA_V2
    return SCHEMA_V1


def _require_text(record: Mapping[str, Any], name: str) -> str:
    value = record.get(name)
    if not isinstance(value, str) or not value:
        raise ContractError(f"字段 {name} 必须是非空字符串", code="invalid_field", details={"field": name})
    return value


def _coerce_value(value: Any) -> float:
    if isinstance(value, bool) or value is None:
        raise ContractError("字段 value 必须是有限数值", code="invalid_field", details={"field": "value"})
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError("字段 value 必须是有限数值", code="invalid_field", details={"field": "value"}) from exc
    if not math.isfinite(number):
        raise ContractError("字段 value 必须是有限数值", code="invalid_field", details={"field": "value"})
    return number


def _check_allowed(raw: Mapping[str, Any], allowed: frozenset[str], version: str) -> None:
    unknown = set(raw) - set(allowed)
    if unknown:
        if unknown & set(ALL_KNOWN_FIELDS):
            raise ContractError(
                f"字段不属于 {version}: " + ",".join(sorted(unknown)),
                code="field_not_allowed",
                details={"fields": sorted(unknown), "schema_version": version},
            )
        raise ContractError(
            "未知字段: " + ",".join(sorted(unknown)),
            code="unknown_field",
            details={"fields": sorted(unknown), "schema_version": version},
        )


def _normalise_contact_channels(raw: Mapping[str, Any]) -> tuple[dict[str, str], ...] | None:
    state = field_presence(raw, "contact_channels")
    if state != PRESENT:
        return None
    value = raw["contact_channels"]
    if not isinstance(value, list):
        raise ContractError("contact_channels 必须是数组或 null", code="invalid_field", details={"field": "contact_channels"})
    channels: list[dict[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise ContractError(
                f"contact_channels[{index}] 必须是对象",
                code="invalid_field",
                details={"field": f"contact_channels[{index}]"},
            )
        unknown = set(item) - set(NESTED_FIELDS["contact_channels"])
        if unknown:
            raise ContractError(
                "contact_channels 中存在未知字段: " + ",".join(sorted(unknown)),
                code="unknown_field",
                details={"fields": [f"contact_channels[{index}].{name}" for name in sorted(unknown)]},
            )
        channel_type = item.get("type")
        channel_value = item.get("value")
        if not isinstance(channel_type, str) or channel_type not in CONTACT_CHANNEL_TYPES:
            raise ContractError(
                f"contact_channels[{index}].type 不是已约定的渠道类型",
                code="unknown_channel_type",
                details={"field": f"contact_channels[{index}].type"},
            )
        if not isinstance(channel_value, str) or not channel_value:
            raise ContractError(
                f"contact_channels[{index}].value 必须是非空字符串",
                code="invalid_field",
                details={"field": f"contact_channels[{index}].value"},
            )
        channels.append({"type": channel_type, "value": channel_value})
    return tuple(channels)


def _normalise_enrollment_source(raw: Mapping[str, Any]) -> dict[str, str | None] | None:
    state = field_presence(raw, "enrollment_source")
    if state != PRESENT:
        return None
    value = raw["enrollment_source"]
    if not isinstance(value, Mapping):
        raise ContractError("enrollment_source 必须是对象或 null", code="invalid_field", details={"field": "enrollment_source"})
    unknown = set(value) - set(NESTED_FIELDS["enrollment_source"])
    if unknown:
        raise ContractError(
            "enrollment_source 中存在未知字段: " + ",".join(sorted(unknown)),
            code="unknown_field",
            details={"fields": [f"enrollment_source.{name}" for name in sorted(unknown)]},
        )
    normalised: dict[str, str | None] = {}
    for name, item in value.items():
        if item is not None and not isinstance(item, str):
            raise ContractError(
                f"enrollment_source.{name} 必须是字符串或 null",
                code="invalid_field",
                details={"field": f"enrollment_source.{name}"},
            )
        normalised[name] = item
    return normalised


def _legacy_contact(
    version: str,
    raw: Mapping[str, Any],
    channels: tuple[dict[str, str], ...] | None,
    *,
    require_projection: bool,
) -> tuple[str | None, str]:
    if version == SCHEMA_V1:
        state = field_presence(raw, "contact")
        value = raw.get("contact")
        if value is not None and not isinstance(value, str):
            raise ContractError("contact 必须是字符串或 null", code="invalid_field", details={"field": "contact"})
        if state == MISSING:
            return None, NOT_COLLECTED
        return value, "input_null" if value is None else "input"

    # A retract only deletes the fact row; it never writes a legacy value, so
    # there is nothing to project and a multi-channel retract stays acceptable.
    if not require_projection:
        return None, NOT_APPLICABLE

    # The legacy DWD column can only be populated without information loss when
    # there is exactly one channel.  Multiple channels are quarantined rather
    # than silently guessed.
    state = field_presence(raw, "contact_channels")
    if channels is None:
        return None, "input_null" if state == NULL else NOT_COLLECTED
    if len(channels) > 1:
        raise ContractError(
            "多个联系渠道无法安全映射到旧列 contact",
            code="ambiguous_legacy_projection",
            details={"field": "contact_channels", "count": len(channels)},
        )
    if not channels:
        return None, "derived"
    return channels[0]["value"], "derived"


def _legacy_source(
    version: str,
    raw: Mapping[str, Any],
    enrollment_source: dict[str, str | None] | None,
) -> tuple[str | None, str]:
    if version == SCHEMA_V1:
        state = field_presence(raw, "source")
        if state == MISSING:
            return "unknown", "default"
        value = raw.get("source")
        if value is not None and not isinstance(value, str):
            raise ContractError("source 必须是字符串或 null", code="invalid_field", details={"field": "source"})
        return value, "input_null" if value is None else "input"

    state = field_presence(raw, "enrollment_source")
    if state == MISSING:
        return "unknown", "default"
    if enrollment_source is None:
        return None, "input_null"
    if "channel" not in enrollment_source:
        return "unknown", "default"
    channel = enrollment_source["channel"]
    if channel is None:
        return None, "input_null"
    if not isinstance(channel, str) or not channel:
        raise ContractError(
            "enrollment_source.channel 必须是非空字符串或 null",
            code="invalid_field",
            details={"field": "enrollment_source.channel"},
        )
    return channel, "derived"


def _input_origin(state: str) -> str:
    if state == PRESENT:
        return "input"
    if state == NULL:
        return "input_null"
    if state == NOT_APPLICABLE:
        return NOT_APPLICABLE
    return NOT_COLLECTED


def parse_event(source: str, batch_id: str, cursor: int, raw: dict) -> Event:
    if not isinstance(raw, Mapping):
        raise ContractError("记录必须是对象", code="invalid_record")

    version = detect_schema_version(raw)
    allowed = V1_FIELDS if version == SCHEMA_V1 else V2_FIELDS
    _check_allowed(raw, allowed, version)

    missing = REQUIRED_FIELDS - set(raw)
    if missing:
        raise ContractError("缺少字段: " + ",".join(sorted(missing)), code="missing_field", details={"fields": sorted(missing)})

    business_key = _require_text(raw, "business_key")
    kind = raw["kind"]
    if kind not in {"upsert", "retract"}:
        raise ContractError("事件类型不受支持", code="invalid_field", details={"field": "kind"})
    occurred_at = _require_text(raw, "occurred_at")
    value = _coerce_value(raw["value"])

    contact_channels = None
    enrollment_source = None
    if version == SCHEMA_V2:
        contact_channels = _normalise_contact_channels(raw)
        enrollment_source = _normalise_enrollment_source(raw)

    contact, contact_origin = _legacy_contact(
        version, raw, contact_channels, require_projection=kind == "upsert"
    )
    legacy_source, source_origin = _legacy_source(version, raw, enrollment_source)

    presence = {
        name: (field_presence(raw, name) if name in allowed else NOT_APPLICABLE)
        for name in ALL_KNOWN_FIELDS
    }

    return Event(
        source=source,
        batch_id=batch_id,
        cursor=cursor,
        business_key=business_key,
        kind=kind,
        value=value,
        occurred_at=occurred_at,
        schema_version=version,
        contact=contact,
        legacy_source=legacy_source,
        contact_channels=contact_channels,
        enrollment_source=enrollment_source,
        presence=presence,
        origins={
            "contact": contact_origin,
            "source": source_origin,
            "contact_channels": _input_origin(presence["contact_channels"]),
            "enrollment_source": _input_origin(presence["enrollment_source"]),
        },
    )
