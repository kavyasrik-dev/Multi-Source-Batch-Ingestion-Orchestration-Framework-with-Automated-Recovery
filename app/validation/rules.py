"""Pure reusable required, type, uniqueness, range and referential rules."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RuleViolation:
    """One record-level or batch-level rule failure."""

    record_index: int | None
    reason: str


class QualityRule(Protocol):
    def evaluate(self, records: Sequence[Mapping[str, object]]) -> tuple[RuleViolation, ...]: ...


@dataclass(frozen=True, slots=True)
class RequiredFieldsRule:
    """Require fields to exist and contain non-null/nonblank values."""

    fields: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_fields(self.fields)

    def evaluate(self, records: Sequence[Mapping[str, object]]) -> tuple[RuleViolation, ...]:
        violations = []
        for index, record in enumerate(records):
            missing = [
                field
                for field in self.fields
                if field not in record
                or record[field] is None
                or (isinstance(record[field], str) and not record[field].strip())
            ]
            if missing:
                violations.append(
                    RuleViolation(
                        index, f"required field(s) missing or blank: {', '.join(missing)}"
                    )
                )
        return tuple(violations)


@dataclass(frozen=True, slots=True)
class SchemaTypesRule:
    """Require configured Python types, with bool never accepted as integer."""

    field_types: Mapping[str, type | tuple[type, ...]]
    nullable_fields: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not isinstance(self.field_types, Mapping) or not self.field_types:
            raise ValueError("field_types must be a non-empty mapping")
        if not all(isinstance(field, str) and field for field in self.field_types):
            raise ValueError("field_types keys must be non-empty strings")
        for expected in self.field_types.values():
            values = expected if isinstance(expected, tuple) else (expected,)
            if not values or not all(isinstance(item, type) for item in values):
                raise ValueError("field_types values must be types or non-empty type tuples")
        if not self.nullable_fields.issubset(self.field_types):
            raise ValueError("nullable_fields must be present in field_types")

    def evaluate(self, records: Sequence[Mapping[str, object]]) -> tuple[RuleViolation, ...]:
        violations = []
        for index, record in enumerate(records):
            errors = []
            for field, expected in self.field_types.items():
                value = record.get(field)
                if value is None and field in self.nullable_fields:
                    continue
                types = expected if isinstance(expected, tuple) else (expected,)
                valid = isinstance(value, types)
                if value is not None and isinstance(value, bool) and int in types:
                    valid = bool in types
                if not valid:
                    expected_names = ", ".join(item.__name__ for item in types)
                    errors.append(f"{field} must be {expected_names}")
            if errors:
                violations.append(RuleViolation(index, "; ".join(errors)))
        return tuple(violations)


@dataclass(frozen=True, slots=True)
class UniquenessRule:
    """Reject every member of a duplicate key group."""

    fields: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_fields(self.fields)

    def evaluate(self, records: Sequence[Mapping[str, object]]) -> tuple[RuleViolation, ...]:
        groups: dict[tuple[object, ...], list[int]] = defaultdict(list)
        invalid = []
        for index, record in enumerate(records):
            try:
                key = tuple(record[field] for field in self.fields)
                hash(key)
            except (KeyError, TypeError):
                invalid.append(RuleViolation(index, "uniqueness key is missing or unhashable"))
                continue
            groups[key].append(index)
        for key, indices in groups.items():
            if len(indices) > 1:
                reason = f"duplicate key {key!r} across record indexes {indices!r}"
                invalid.extend(RuleViolation(index, reason) for index in indices)
        return tuple(sorted(invalid, key=lambda item: item.record_index))


@dataclass(frozen=True, slots=True)
class RangeRule:
    """Require comparable values to stay within inclusive optional bounds."""

    field: str
    minimum: object | None = None
    maximum: object | None = None

    def __post_init__(self) -> None:
        if not self.field:
            raise ValueError("range field must be non-empty")
        if self.minimum is None and self.maximum is None:
            raise ValueError("range rule requires a minimum or maximum")
        if self.minimum is not None and self.maximum is not None:
            try:
                invalid = self.minimum > self.maximum
            except TypeError as exc:
                raise ValueError("range bounds must be comparable") from exc
            if invalid:
                raise ValueError("range minimum cannot exceed maximum")

    def evaluate(self, records: Sequence[Mapping[str, object]]) -> tuple[RuleViolation, ...]:
        violations = []
        for index, record in enumerate(records):
            value = record.get(self.field)
            try:
                below = self.minimum is not None and value < self.minimum
                above = self.maximum is not None and value > self.maximum
            except TypeError:
                violations.append(
                    RuleViolation(index, f"{self.field} is missing or not comparable to bounds")
                )
                continue
            if below or above:
                violations.append(
                    RuleViolation(
                        index,
                        f"{self.field}={value!r} outside inclusive range "
                        f"[{self.minimum!r}, {self.maximum!r}]",
                    )
                )
        return tuple(violations)


@dataclass(frozen=True, slots=True)
class ReferentialRule:
    """Require a field value to exist in a reference set when an optional condition applies."""

    field: str
    allowed_values: frozenset[object]
    when_field: str | None = None
    when_values: frozenset[object] | None = None

    def __post_init__(self) -> None:
        if not self.field or not self.allowed_values:
            raise ValueError("referential field and allowed_values are required")
        if (self.when_field is None) != (self.when_values is None):
            raise ValueError("when_field and when_values must be supplied together")
        if self.when_values is not None and not self.when_values:
            raise ValueError("when_values cannot be empty")

    def evaluate(self, records: Sequence[Mapping[str, object]]) -> tuple[RuleViolation, ...]:
        violations = []
        for index, record in enumerate(records):
            if self.when_field is not None:
                try:
                    applies = record.get(self.when_field) in self.when_values
                except TypeError:
                    applies = False
                if not applies:
                    continue
            value = record.get(self.field)
            try:
                valid = value in self.allowed_values
            except TypeError:
                valid = False
            if not valid:
                violations.append(
                    RuleViolation(index, f"{self.field}={value!r} not found in reference set")
                )
        return tuple(violations)


def _validate_fields(fields: tuple[str, ...]) -> None:
    if (
        not fields
        or len(set(fields)) != len(fields)
        or not all(isinstance(field, str) and field for field in fields)
    ):
        raise ValueError("fields must contain unique non-empty strings")
