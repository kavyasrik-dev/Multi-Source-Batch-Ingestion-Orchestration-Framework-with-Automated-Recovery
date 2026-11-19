"""Strict, streaming CSV source adapter with stable file and batch identity."""

import codecs
import csv
import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname
from uuid import NAMESPACE_URL, uuid5

from app.connectors.base import ExtractedBatch, SourceMetadata, SourceObject, SourceRecord


class CSVAdapterError(ValueError):
    """Base error for CSV discovery, decoding, structure, or source changes."""


class DuplicateInputError(CSVAdapterError):
    """The same resolved input file was discovered more than once."""


class CSVHeaderError(CSVAdapterError):
    """A header is missing, duplicated, blank, or differs from the configured contract."""


class CSVRecordError(CSVAdapterError):
    """A physical CSV record is malformed or has the wrong number of fields."""


class SourceChangedError(CSVAdapterError):
    """The source file changed while it was being extracted."""


@dataclass(frozen=True, slots=True)
class CSVAdapterConfig:
    """CSV discovery and parsing policy."""

    paths: tuple[Path, ...]
    delimiter: str = ","
    encoding: str = "utf-8"
    has_header: bool = True
    expected_headers: tuple[str, ...] | None = None
    recursive: bool = False

    def __post_init__(self) -> None:
        if not self.paths:
            raise ValueError("at least one CSV path is required")
        if not all(isinstance(path, Path) for path in self.paths):
            raise TypeError("paths must contain pathlib.Path values")
        if not isinstance(self.delimiter, str) or len(self.delimiter) != 1:
            raise ValueError("delimiter must be exactly one character")
        if self.delimiter in {"\r", "\n", '"'}:
            raise ValueError("delimiter cannot be a newline or quote character")
        try:
            codecs.lookup(self.encoding)
        except LookupError as exc:
            raise ValueError(f"unknown encoding: {self.encoding}") from exc
        if self.expected_headers is not None:
            _validate_headers(self.expected_headers, source="configured expected headers")
        if not self.has_header and self.expected_headers is None:
            raise ValueError("expected_headers is required when has_header is false")

    @classmethod
    def from_path(cls, path: Path, **kwargs) -> "CSVAdapterConfig":
        """Build the common single-path configuration without a one-item tuple at call sites."""
        return cls(paths=(path,), **kwargs)


def _validate_headers(headers: tuple[str, ...], *, source: str) -> None:
    if not headers:
        raise CSVHeaderError(f"{source}: header cannot be empty")
    blank = [str(index + 1) for index, value in enumerate(headers) if not value]
    if blank:
        raise CSVHeaderError(f"{source}: blank header at column(s) {', '.join(blank)}")
    duplicates = sorted({value for value in headers if headers.count(value) > 1})
    if duplicates:
        raise CSVHeaderError(f"{source}: duplicate header(s): {', '.join(duplicates)}")


class CSVAdapter:
    """Discover local CSV files and stream strictly validated, bounded row batches."""

    def __init__(self, config: CSVAdapterConfig) -> None:
        self.config = config

    def discover(self) -> tuple[SourceObject, ...]:
        """Resolve files and directories, rejecting duplicate resolved paths."""
        paths: list[Path] = []
        for configured in self.config.paths:
            if not configured.exists():
                raise CSVAdapterError(f"CSV input does not exist: {configured}")
            if configured.is_file():
                paths.append(configured.resolve())
                continue
            if not configured.is_dir():
                raise CSVAdapterError(f"CSV input is not a regular file or directory: {configured}")
            pattern = "**/*.csv" if self.config.recursive else "*.csv"
            paths.extend(path.resolve() for path in configured.glob(pattern) if path.is_file())
        ordered = sorted(paths, key=lambda path: path.as_posix().casefold())
        seen: set[str] = set()
        objects = []
        for path in ordered:
            identity = path.as_posix().casefold()
            if identity in seen:
                raise DuplicateInputError(f"CSV input discovered more than once: {path}")
            seen.add(identity)
            objects.append(SourceObject(locator=path.as_uri(), name=path.name))
        return tuple(objects)

    @staticmethod
    def _path(source: SourceObject) -> Path:
        parsed = urlparse(source.locator)
        if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
            raise CSVAdapterError(f"CSV source locator is not a local file URI: {source.locator}")
        try:
            path = Path(url2pathname(unquote(parsed.path))).resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise CSVAdapterError(f"CSV source is unavailable: {source.locator}") from exc
        if path.as_uri() != source.locator or not path.is_file():
            raise CSVAdapterError(
                f"CSV source locator is not a canonical file URI: {source.locator}"
            )
        return path

    def metadata(self, source: SourceObject) -> SourceMetadata:
        """Hash the complete file and bind identity to canonical URI, bytes, and content."""
        path = self._path(source)
        before = path.stat()
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise SourceChangedError(f"CSV source changed while checksumming: {path}")
        checksum = digest.hexdigest()
        identity = uuid5(NAMESPACE_URL, f"csv\0{source.locator}\0{after.st_size}\0{checksum}")
        return SourceMetadata(
            identity=identity,
            checksum_sha256=checksum,
            byte_count=after.st_size,
            modified_at=datetime.fromtimestamp(after.st_mtime, tz=UTC),
            media_type="text/csv",
        )

    def _records(
        self, source: SourceObject, *, expected_metadata: SourceMetadata | None = None
    ) -> Iterator[SourceRecord]:
        path = self._path(source)
        initial = path.stat()
        headers = self.config.expected_headers if not self.config.has_header else None
        record_number = 0
        try:
            with path.open("r", encoding=self.config.encoding, newline="") as stream:
                reader = csv.reader(stream, delimiter=self.config.delimiter, strict=True)
                if self.config.has_header:
                    try:
                        first = next(reader)
                    except StopIteration:
                        return
                    headers = tuple(first)
                    _validate_headers(headers, source=f"{path}: line 1")
                    if (
                        self.config.expected_headers is not None
                        and headers != self.config.expected_headers
                    ):
                        raise CSVHeaderError(
                            f"{path}: unexpected headers; expected "
                            f"{self.config.expected_headers!r}, received {headers!r}"
                        )
                assert headers is not None
                for row in reader:
                    record_number += 1
                    if len(row) != len(headers):
                        raise CSVRecordError(
                            f"{path}: line {reader.line_num}: expected {len(headers)} fields, "
                            f"received {len(row)}; row={row!r}"
                        )
                    yield SourceRecord(
                        values=MappingProxyType(dict(zip(headers, row, strict=True))),
                        line_number=reader.line_num,
                    )
        except UnicodeDecodeError as exc:
            raise CSVAdapterError(
                f"{path}: cannot decode with {self.config.encoding} "
                f"at byte {exc.start}: {exc.reason}"
            ) from exc
        except csv.Error as exc:
            raise CSVRecordError(
                f"{path}: malformed CSV near line {record_number + 1}: {exc}"
            ) from exc
        final = path.stat()
        if (initial.st_size, initial.st_mtime_ns) != (final.st_size, final.st_mtime_ns):
            raise SourceChangedError(f"CSV source changed during extraction: {path}")
        if expected_metadata is not None and (
            final.st_size != expected_metadata.byte_count
            or self.metadata(source).checksum_sha256 != expected_metadata.checksum_sha256
        ):
            raise SourceChangedError(
                f"CSV source identity changed before extraction completed: {path}"
            )

    def extract(self, source: SourceObject) -> Iterator[SourceRecord]:
        """Stream validated rows; consume the iterator to run final change checks."""
        return self._records(source)

    def iter_batches(self, source: SourceObject, *, max_records: int) -> Iterator[ExtractedBatch]:
        """Yield stable, bounded non-empty batches and verify source identity after extraction."""
        if isinstance(max_records, bool) or not isinstance(max_records, int) or max_records <= 0:
            raise ValueError("max_records must be a positive integer")
        metadata = self.metadata(source)
        schema = "\x1f".join(
            (
                codecs.lookup(self.config.encoding).name,
                self.config.delimiter,
                str(self.config.has_header),
                *(self.config.expected_headers or ()),
            )
        )
        records: list[SourceRecord] = []
        start = 0
        for record in self._records(source, expected_metadata=metadata):
            records.append(record)
            if len(records) == max_records:
                yield self._batch(source, metadata, start, tuple(records), schema)
                start += len(records)
                records.clear()
        if records:
            yield self._batch(source, metadata, start, tuple(records), schema)

    @staticmethod
    def _batch(
        source: SourceObject,
        metadata: SourceMetadata,
        start: int,
        records: tuple[SourceRecord, ...],
        schema: str,
    ) -> ExtractedBatch:
        end = start + len(records)
        identity = f"csv-batch\0{metadata.identity}\0{schema}\0{start}\0{end}"
        return ExtractedBatch(
            batch_id=uuid5(NAMESPACE_URL, identity),
            source=source,
            source_metadata=metadata,
            start_record=start,
            records=records,
        )
