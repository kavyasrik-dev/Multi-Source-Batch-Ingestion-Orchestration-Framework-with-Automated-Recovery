"""Canonical order contract v1 and explicit source-to-canonical conversions."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

ORDER_SCHEMA_VERSION = 1
_ALLOWED_FIELDS = frozenset(
    {
        "schema_version",
        "order_id",
        "customer_id",
        "order_timestamp",
        "amount",
        "currency",
        "status",
        "source_system",
        "ingested_at",
        "batch_id",
        "notes",
    }
)
_REQUIRED_FIELDS = _ALLOWED_FIELDS - {"status", "notes"}
_STATUSES = frozenset({"pending", "confirmed", "fulfilled", "cancelled", "refunded"})
_MAX_AMOUNT = Decimal("9999999999999999.99")


class RecordValidationError(ValueError):
    """One canonical record failed validation with actionable field messages."""

    def __init__(self, errors: dict[str, str]) -> None:
        self.errors = errors
        super().__init__("; ".join(f"{field}: {message}" for field, message in errors.items()))


@dataclass(frozen=True, slots=True)
class OrderRecord:
    """Canonical order v1; optional status and notes are nullable."""

    schema_version: int
    order_id: UUID
    customer_id: str
    order_timestamp: datetime
    amount: Decimal
    currency: str
    source_system: str
    ingested_at: datetime
    batch_id: UUID
    status: str | None = None
    notes: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return values in canonical schema order for Arrow construction."""
        return {
            "schema_version": self.schema_version,
            "order_id": str(self.order_id),
            "customer_id": self.customer_id,
            "order_timestamp": self.order_timestamp,
            "amount": self.amount,
            "currency": self.currency,
            "status": self.status,
            "source_system": self.source_system,
            "ingested_at": self.ingested_at,
            "batch_id": str(self.batch_id),
            "notes": self.notes,
        }


def _required_text(value: Any, field: str, errors: dict[str, str]) -> str | None:
    if not isinstance(value, str) or not value.strip():
        errors[field] = "must be a non-empty string"
        return None
    return value.strip()


def _uuid(value: Any, field: str, errors: dict[str, str]) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str):
        errors[field] = "must be a UUID or canonical UUID string"
        return None
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError):
        errors[field] = "must be a valid UUID"
        return None
    if str(parsed) != value:
        errors[field] = "must use canonical 8-4-4-4-12 UUID form"
        return None
    return parsed


def _timestamp(value: Any, field: str, errors: dict[str, str]) -> datetime | None:
    parsed = value
    if isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            errors[field] = "must be an ISO 8601 timestamp with an explicit UTC offset"
            return None
    if not isinstance(parsed, datetime):
        errors[field] = "must be a datetime or ISO 8601 timestamp string"
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        errors[field] = "must include a timezone offset; naive timestamps are ambiguous"
        return None
    return parsed.astimezone(UTC)


def _amount(value: Any, errors: dict[str, str]) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        errors["amount"] = "must be an integer, Decimal, or decimal string; floats are rejected"
        return None
    try:
        parsed = Decimal(value.strip() if isinstance(value, str) else value)
    except (InvalidOperation, ValueError):
        errors["amount"] = "must be a finite base-10 number"
        return None
    if not parsed.is_finite() or parsed < 0:
        errors["amount"] = "must be finite and non-negative"
        return None
    if parsed.as_tuple().exponent < -2:
        errors["amount"] = "must have no more than 2 fractional digits; rounding is not implicit"
        return None
    parsed = parsed.quantize(Decimal("0.01"))
    if parsed > _MAX_AMOUNT:
        errors["amount"] = "exceeds decimal(18,2) capacity"
        return None
    return parsed


def parse_order(data: Any) -> OrderRecord:
    """Parse one mapping into canonical v1 without lossy or locale-dependent coercions."""
    if not isinstance(data, dict):
        raise RecordValidationError({"record": "must be an object/mapping"})
    errors: dict[str, str] = {}
    unknown = sorted(set(data) - _ALLOWED_FIELDS)
    if unknown:
        errors["record"] = f"unknown fields: {', '.join(unknown)}"
    for field in sorted(_REQUIRED_FIELDS - set(data)):
        errors[field] = "is required"

    version = data.get("schema_version")
    if isinstance(version, bool) or not isinstance(version, int) or version != ORDER_SCHEMA_VERSION:
        errors["schema_version"] = f"must equal {ORDER_SCHEMA_VERSION}"
    order_id = _uuid(data.get("order_id"), "order_id", errors)
    batch_id = _uuid(data.get("batch_id"), "batch_id", errors)
    customer_id = _required_text(data.get("customer_id"), "customer_id", errors)
    order_timestamp = _timestamp(data.get("order_timestamp"), "order_timestamp", errors)
    ingested_at = _timestamp(data.get("ingested_at"), "ingested_at", errors)
    amount = _amount(data.get("amount"), errors)
    currency = _required_text(data.get("currency"), "currency", errors)
    if currency is not None:
        currency = currency.upper()
        if len(currency) != 3 or not currency.isascii() or not currency.isalpha():
            errors["currency"] = "must be a 3-letter ASCII currency code"
    source = _required_text(data.get("source_system"), "source_system", errors)
    status = data.get("status")
    if status is not None and (not isinstance(status, str) or status not in _STATUSES):
        errors["status"] = f"must be null or one of: {', '.join(sorted(_STATUSES))}"
    notes = data.get("notes")
    if notes is not None and not isinstance(notes, str):
        errors["notes"] = "must be a string or null"
    if errors:
        raise RecordValidationError(errors)
    return OrderRecord(
        schema_version=version,
        order_id=order_id,
        customer_id=customer_id,
        order_timestamp=order_timestamp,
        amount=amount,
        currency=currency,
        status=status,
        source_system=source,
        ingested_at=ingested_at,
        batch_id=batch_id,
        notes=notes,
    )


def parse_orders(rows: list[dict[str, Any]]) -> list[OrderRecord]:
    """Parse a batch and report every invalid row with its zero-based index."""
    parsed, batch_errors = [], {}
    for index, row in enumerate(rows):
        try:
            parsed.append(parse_order(row))
        except RecordValidationError as exc:
            batch_errors.update(
                {f"records[{index}].{field}": message for field, message in exc.errors.items()}
            )
    if batch_errors:
        raise RecordValidationError(batch_errors)
    return parsed
