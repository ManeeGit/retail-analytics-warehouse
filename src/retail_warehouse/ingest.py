"""Preserve original bytes and ingest CSV/XLSX into a typed, auditable bronze file."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
import tempfile
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

import duckdb

from .fixtures import HEADERS
from .storage import atomic_json, sha256, utcnow, writer_lock

BRONZE_DDL = """(
 source_row_id VARCHAR PRIMARY KEY, source_sha256 VARCHAR, source_sheet VARCHAR,
 source_row_number BIGINT, invoice_no VARCHAR, stock_code VARCHAR, description VARCHAR,
 quantity BIGINT, invoice_at TIMESTAMP, unit_price DECIMAL(18,4), customer_id VARCHAR,
 country VARCHAR, signed_quantity BIGINT, net_amount DECIMAL(18,2), is_cancellation BOOLEAN,
 rejection_reason VARCHAR, raw_json VARCHAR
)"""


def text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def parse_date(value: str) -> datetime:
    # UCI Excel dates have no timezone; keep source-local naive wall-clock time.
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError("invalid timestamp")


def normalize(raw: dict, file_hash: str, sheet: str, row_number: int) -> tuple:
    raw = {key: text(value) for key, value in raw.items()}
    errors = []
    invoice, stock = raw["InvoiceNo"], raw["StockCode"]
    if not invoice:
        errors.append("missing_invoice")
    if not stock:
        errors.append("missing_stock")
    quantity = price = timestamp = amount = signed = None
    try:
        value = raw["Quantity"]
        if not re.fullmatch(r"[+-]?\d+", value):
            raise ValueError
        quantity = int(value)
        if not quantity or abs(quantity) > 1_000_000_000:
            raise ValueError
    except ValueError:
        errors.append("invalid_quantity")
        quantity = None
    try:
        price = Decimal(raw["UnitPrice"])
        if not price.is_finite() or price < 0 or price >= Decimal("1000000000"):
            raise ValueError
        if price != price.quantize(Decimal("0.0001")):
            raise ValueError
    except (InvalidOperation, ValueError):
        errors.append("invalid_price")
        price = None
    try:
        timestamp = parse_date(raw["InvoiceDate"])
        # Match the half-open customer-history domain used by the as-of join.
        if not datetime(1900, 1, 1) <= timestamp < datetime(9999, 12, 31):
            raise ValueError("timestamp outside supported dimension range")
    except ValueError:
        errors.append("invalid_timestamp")
        timestamp = None
    cancellation = invoice.upper().startswith("C")
    if quantity is not None:
        signed = -abs(quantity) if cancellation else quantity
    if signed is not None and price is not None:
        amount = (price * signed).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if abs(amount) >= Decimal("10000000000000000"):
            errors.append("amount_overflow")
            amount = None
    # Positional source identity preserves legitimate repeated identical invoice lines.
    row_id = hashlib.sha256(f"{file_hash}:{sheet}:{row_number}".encode()).hexdigest()
    return (row_id, file_hash, sheet, row_number, invoice, stock, raw["Description"] or "Unknown",
            quantity, timestamp, price, raw["CustomerID"] or "UNKNOWN", raw["Country"] or "Unknown",
            signed, amount, cancellation, ";".join(errors) or None, json.dumps(raw, sort_keys=True))


def source_rows(path: Path):
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames is None or len(set(reader.fieldnames)) != len(reader.fieldnames) or not set(HEADERS).issubset(reader.fieldnames):
                raise ValueError(f"CSV requires unique columns {HEADERS}")
            for number, row in enumerate(reader, start=2):
                if None in row:
                    raise ValueError(f"Extra columns at row {number}")
                yield "csv", number, row
    elif path.suffix.lower() == ".xlsx":
        from openpyxl import load_workbook
        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            for sheet in workbook:
                rows = sheet.iter_rows(values_only=True)
                header = [text(value) for value in next(rows, ())]
                if not set(HEADERS).issubset(header) or len(set(header)) != len(header):
                    raise ValueError(f"Worksheet {sheet.title} does not have the required unique columns")
                for number, values in enumerate(rows, start=2):
                    if all(value is None for value in values):
                        continue
                    yield sheet.title, number, dict(zip(header, values))
        finally:
            workbook.close()
    else:
        raise ValueError("Input must be a CSV or XLSX file")


def verify_manifest(directory: Path) -> dict:
    manifest = json.loads((directory / "manifest.json").read_text())
    if sha256(directory / manifest["original_file"]) != manifest["source_sha256"]:
        raise ValueError(f"Raw source checksum mismatch: {directory.name}")
    if sha256(directory / "bronze.parquet") != manifest["parquet_sha256"]:
        raise ValueError(f"Parquet checksum mismatch: {directory.name}")
    return manifest


def ingest(root: Path, source: Path, *, provenance: str = "user-provided") -> dict:
    if not source.is_file():
        raise FileNotFoundError(source)
    if source.suffix.lower() not in (".csv", ".xlsx"):
        raise ValueError("Input must be a CSV or XLSX file")
    with writer_lock(root):
        # Snapshot first so original bytes, hash, and parsed values always agree.
        raw_root = root / "raw"
        raw_root.mkdir(exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".ingest-", dir=raw_root))
        try:
            original = staging / ("source" + source.suffix.lower())
            shutil.copyfile(source, original)
            digest = sha256(original)
            destination = raw_root / digest
            if destination.exists():
                return verify_manifest(destination)
            connection = duckdb.connect()
            try:
                connection.execute("SET memory_limit='512MB'")
                connection.execute("SET threads=2")
                connection.execute("CREATE TABLE bronze " + BRONZE_DDL)
                batch = []
                for sheet, number, raw in source_rows(original):
                    batch.append(normalize(raw, digest, sheet, number))
                    if len(batch) == 5000:
                        connection.executemany("INSERT INTO bronze VALUES (" + ",".join(["?"] * 17) + ")", batch)
                        batch.clear()
                if batch:
                    connection.executemany("INSERT INTO bronze VALUES (" + ",".join(["?"] * 17) + ")", batch)
                count, rejected, minimum, maximum = connection.execute("SELECT count(*), count(rejection_reason), min(invoice_at), max(invoice_at) FROM bronze").fetchone()
                if not count:
                    raise ValueError("The source contains no data rows")
                connection.execute("COPY bronze TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(staging / "bronze.parquet")])
            finally:
                connection.close()
            manifest = {"schema_version": 1, "source_name": source.name, "original_file": original.name,
                        "source_sha256": digest, "parquet_sha256": sha256(staging / "bronze.parquet"),
                        "row_count": count, "rejected_count": rejected, "min_event_at": minimum.isoformat() if minimum else None,
                        "max_event_at": maximum.isoformat() if maximum else None, "ingested_at": utcnow(), "provenance": provenance}
            atomic_json(staging / "manifest.json", manifest)
            staging.rename(destination)
            return manifest
        finally:
            if staging.exists():
                shutil.rmtree(staging)
