import csv
import json
from decimal import Decimal

import duckdb
import pytest
from openpyxl import Workbook

from retail_warehouse.fixtures import HEADERS, ROWS, generate
from retail_warehouse.ingest import ingest, normalize, verify_manifest
from retail_warehouse.storage import sha256


def test_fixture_is_deterministic(tmp_path):
    first = generate(tmp_path / "a")
    second = generate(tmp_path / "b")
    assert [sha256(p) for p in first] == [sha256(p) for p in second]


@pytest.mark.parametrize("field,value,reason", [
    ("UnitPrice", "NaN", "invalid_price"),
    ("UnitPrice", "Infinity", "invalid_price"),
    ("UnitPrice", "-1", "invalid_price"),
    ("UnitPrice", "0.12345", "invalid_price"),
    ("Quantity", "0", "invalid_quantity"),
    ("Quantity", "1.5", "invalid_quantity"),
    ("Quantity", "1000000001", "invalid_quantity"),
    ("InvoiceDate", "garbage", "invalid_timestamp"),
    ("InvoiceDate", "1899-12-31", "invalid_timestamp"),
    ("InvoiceDate", "9999-12-31", "invalid_timestamp"),
    ("InvoiceNo", "", "missing_invoice"),
    ("StockCode", "", "missing_stock"),
])
def test_invalid_values_are_quarantined(field, value, reason):
    raw = dict(zip(HEADERS, ROWS[0]))
    raw[field] = value
    row = normalize(raw, "file-hash", "csv", 2)
    assert reason in row[15]


def test_cancellation_decimal_missing_and_identity():
    raw = dict(zip(HEADERS, ROWS[0]))
    raw.update(InvoiceNo="C123", Quantity="3", UnitPrice="0.335", CustomerID="", Country="")
    row = normalize(raw, "file-hash", "csv", 2)
    assert row[12] == -3
    assert row[13] == Decimal("-1.01")
    assert row[10:12] == ("UNKNOWN", "Unknown")
    assert row[0] != normalize(raw, "file-hash", "csv", 3)[0]
    assert row[0] == normalize(raw, "file-hash", "csv", 2)[0]


def test_ingest_idempotence_and_audit(tmp_path):
    source, _ = generate(tmp_path / "fixtures")
    root = tmp_path / "warehouse"
    first = ingest(root, source)
    second = ingest(root, source)
    assert first == second
    assert first["row_count"] == 17
    assert first["rejected_count"] == 4
    directory = root / "raw" / first["source_sha256"]
    assert verify_manifest(directory) == first
    con = duckdb.connect()
    rows, unique = con.execute("SELECT count(*), count(distinct source_row_id) FROM read_parquet(?)", [str(directory / "bronze.parquet")]).fetchone()
    con.close()
    assert rows == unique == 17
    assert json.loads((directory / "manifest.json").read_text())["source_name"] == source.name


def test_manifest_detects_tampering(tmp_path):
    source, _ = generate(tmp_path / "fixtures")
    manifest = ingest(tmp_path / "warehouse", source)
    directory = tmp_path / "warehouse" / "raw" / manifest["source_sha256"]
    with (directory / "source.csv").open("a") as stream:
        stream.write("tampering")
    with pytest.raises(ValueError, match="checksum mismatch"):
        verify_manifest(directory)


def test_xlsx_streaming_source(tmp_path):
    source = tmp_path / "input.xlsx"
    book = Workbook()
    book.active.title = "Online Retail"
    book.active.append(HEADERS)
    book.active.append(ROWS[0])
    book.save(source)
    manifest = ingest(tmp_path / "warehouse", source)
    assert manifest["row_count"] == 1
    assert manifest["rejected_count"] == 0


def test_bad_headers_leave_no_manifest(tmp_path):
    source = tmp_path / "wrong.csv"
    source.write_text("not,the,schema\n1,2,3\n")
    root = tmp_path / "warehouse"
    with pytest.raises(ValueError, match="unique columns"):
        ingest(root, source)
    assert not list((root / "raw").glob("*/manifest.json"))


def test_empty_file_rejected(tmp_path):
    source = tmp_path / "empty.csv"
    with source.open("w") as stream:
        csv.writer(stream).writerow(HEADERS)
    with pytest.raises(ValueError, match="no data rows"):
        ingest(tmp_path / "warehouse", source)


@pytest.mark.parametrize("value", ["1900-01-01", "9999-12-30 23:59:59"])
def test_timestamp_inside_dimension_domain_is_accepted(value):
    raw = dict(zip(HEADERS, ROWS[0]))
    raw["InvoiceDate"] = value
    row = normalize(raw, "file-hash", "csv", 2)
    assert row[15] is None
    assert row[8] is not None
