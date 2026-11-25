"""Isolated source-to-canonical order mapping and explicit conversions."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from uuid import NAMESPACE_URL, UUID, uuid5

from app.common.contracts.orders import (
    ORDER_SCHEMA_VERSION,
    OrderRecord,
    RecordValidationError,
    parse_order,
)
from app.connectors.base import SourceRecord

_CANONICAL_STATUSES = frozenset({"pending", "confirmed", "fulfilled", "cancelled", "refunded"})


@dataclass(frozen=True, slots=True)
class OrderMapping:
    """Source field mapping and deterministic normalization rules for canonical orders."""

    source_system: str
    order_id_field: str
    customer_id_field: str
    order_timestamp_field: str
    amount_field: str
    currency_field: str
    status_field: str | None = None
    notes_field: str | None = None
    order_id_mode: str = "uuid"
    timestamp_formats: tuple[str, ...] = ("iso8601",)
    status_aliases: Mapping[str, str] = field(default_factory=dict)
    currency_aliases: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        required = (
            self.source_system,
            self.order_id_field,
            self.customer_id_field,
            self.order_timestamp_field,
            self.amount_field,
            self.currency_field,
        )
        if not all(isinstance(value, str) and value.strip() for value in required):
            raise ValueError("source system and required source fields must be non-empty strings")
        if self.status_field is not None and not self.status_field:
            raise ValueError("status_field must be null or a non-empty string")
        if self.notes_field is not None and not self.notes_field:
            raise ValueError("notes_field must be null or a non-empty string")
        if self.order_id_mode not in {"uuid", "uuid5"}:
            raise ValueError("order_id_mode must be 'uuid' or 'uuid5'")
        if not self.timestamp_formats or not all(
            isinstance(value, str) and value for value in self.timestamp_formats
        ):
            raise ValueError("timestamp_formats must contain at least one format")
        statuses = _normalize_aliases(self.status_aliases, "status_aliases")
        if not all(value in _CANONICAL_STATUSES for value in statuses.values()):
            raise ValueError("status_aliases values must be canonical order statuses")
        currencies = _normalize_aliases(self.currency_aliases, "currency_aliases")
        if not all(
            len(value) == 3 and value.isascii() and value.isalpha() for value in currencies.values()
        ):
            raise ValueError("currency_aliases values must be 3-letter ASCII codes")
        object.__setattr__(self, "source_system", self.source_system.strip())
        object.__setattr__(self, "status_aliases", MappingProxyType(statuses))
        object.__setattr__(
            self,
            "currency_aliases",
            MappingProxyType({key: value.upper() for key, value in currencies.items()}),
        )


def _normalize_aliases(values: Mapping[str, str], label: str) -> dict[str, str]:
    if not isinstance(values, Mapping) or not all(
        isinstance(key, str) and key.strip() and isinstance(value, str) and value.strip()
        for key, value in values.items()
    ):
        raise ValueError(f"{label} must map non-empty strings to non-empty strings")
    return {key.strip().casefold(): value.strip().casefold() for key, value in values.items()}


class OrderTransformer:
    """Transform one extracted source record without performing I/O."""

    def __init__(self, mapping: OrderMapping) -> None:
        self.mapping = mapping

    def transform(
        self,
        record: SourceRecord,
        *,
        batch_id: UUID,
        ingested_at: datetime,
    ) -> OrderRecord:
        """Map and validate one row, raising only actionable record validation errors."""
        if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
            raise ValueError("ingested_at must be timezone-aware")
        source = record.values
        order_id = self._required(source, self.mapping.order_id_field, "order_id")
        if self.mapping.order_id_mode == "uuid5":
            if not isinstance(order_id, (str, int)) or isinstance(order_id, bool):
                raise RecordValidationError(
                    {"order_id": "uuid5 source identifier must be a string or integer"}
                )
            text = str(order_id).strip()
            if not text:
                raise RecordValidationError({"order_id": "source identifier cannot be empty"})
            order_id = str(uuid5(NAMESPACE_URL, f"{self.mapping.source_system}\0{text}"))
        timestamp = self._timestamp(
            self._required(source, self.mapping.order_timestamp_field, "order_timestamp")
        )
        currency = self._category(
            self._required(source, self.mapping.currency_field, "currency"),
            "currency",
            self.mapping.currency_aliases,
            uppercase=True,
        )
        status = None
        if self.mapping.status_field is not None:
            raw_status = source.get(self.mapping.status_field)
            if raw_status is not None and raw_status != "":
                status = self._category(
                    raw_status,
                    "status",
                    self.mapping.status_aliases,
                    uppercase=False,
                )
        notes = source.get(self.mapping.notes_field) if self.mapping.notes_field else None
        canonical = {
            "schema_version": ORDER_SCHEMA_VERSION,
            "order_id": order_id,
            "customer_id": self._required(source, self.mapping.customer_id_field, "customer_id"),
            "order_timestamp": timestamp,
            "amount": self._required(source, self.mapping.amount_field, "amount"),
            "currency": currency,
            "status": status,
            "source_system": self.mapping.source_system,
            "ingested_at": ingested_at.astimezone(UTC),
            "batch_id": str(batch_id),
            "notes": notes,
        }
        return parse_order(canonical)

    @staticmethod
    def _required(source: Mapping[str, object], source_field: str, canonical_field: str) -> object:
        if source_field not in source:
            raise RecordValidationError(
                {canonical_field: f"source field '{source_field}' is missing"}
            )
        return source[source_field]

    def _timestamp(self, value: object) -> datetime:
        if isinstance(value, datetime):
            parsed = value
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise RecordValidationError({"order_timestamp": "must include a timezone offset"})
            return parsed.astimezone(UTC)
        if not isinstance(value, str):
            raise RecordValidationError(
                {"order_timestamp": "must be a datetime or configured timestamp string"}
            )
        failures = []
        for format_value in self.mapping.timestamp_formats:
            try:
                if format_value == "iso8601":
                    text = value.strip()
                    if text.endswith(("Z", "z")):
                        text = text[:-1] + "+00:00"
                    parsed = datetime.fromisoformat(text)
                else:
                    parsed = datetime.strptime(value.strip(), format_value)
            except ValueError:
                failures.append(format_value)
                continue
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise RecordValidationError(
                    {"order_timestamp": "configured timestamp must include a timezone offset"}
                )
            return parsed.astimezone(UTC)
        raise RecordValidationError(
            {"order_timestamp": "does not match configured format(s): " + ", ".join(failures)}
        )

    @staticmethod
    def _category(
        value: object,
        field_name: str,
        aliases: Mapping[str, str],
        *,
        uppercase: bool,
    ) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RecordValidationError({field_name: "must be a non-empty string"})
        normalized = value.strip().casefold()
        normalized = aliases.get(normalized, normalized)
        return normalized.upper() if uppercase else normalized
