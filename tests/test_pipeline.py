import json
from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from retail_warehouse import pipeline
from retail_warehouse.fixtures import generate
from retail_warehouse.ingest import ingest
from retail_warehouse.pipeline import rollback, run, select_manifest, verify_release
from retail_warehouse.storage import current_release, writer_lock


def query(root, sql):
    con = duckdb.connect(str(current_release(root) / "warehouse.duckdb"), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    base = tmp_path_factory.mktemp("integration")
    fixtures = generate(base / "fixtures")
    root = base / "warehouse"
    ingest(root, fixtures[0])
    report = run(root, retries=0)
    return root, fixtures, report


def test_real_dbt_star_schema_and_revenue(built):
    root, _, report = built
    assert report["row_counts"]["fct_invoice_line"] == 13
    assert report["row_counts"]["quarantine"] == 4
    assert query(root, "SELECT sum(net_amount) FROM fct_invoice_line") == [(Decimal("33.99"),)]
    assert query(root, "SELECT count(*) FROM fct_invoice_line WHERE invoice_no='10009'") == [(2,)]
    assert query(root, "SELECT count(*) FROM fct_invoice_line WHERE customer_id='UNKNOWN'") == [(1,)]
    assert query(root, "SELECT count(*) FROM fct_invoice_line WHERE is_return") == [(3,)]
    assert verify_release(current_release(root))["release_id"] == report["release_id"]
    results = json.loads((current_release(root) / "dbt/target/run_results.json").read_text())
    assert len(results["results"]) >= 20
    assert all(r["status"] in ("pass", "success") for r in results["results"])


def test_scd2_as_of_join(built):
    root, _, _ = built
    sql = """SELECT distinct c.segment, cast(f.invoice_at as date)
             FROM fct_invoice_line f JOIN dim_customer c USING(customer_key)
             WHERE f.customer_id='101' ORDER BY 2"""
    rows = query(root, sql)
    assert rows[0][0] == "standard"
    assert rows[-1][0] == "loyal"
    assert query(root, "SELECT count(*) FROM dim_customer WHERE customer_id='101'") == [(2,)]


def test_failure_preserves_current_and_records_error(built):
    root, _, _ = built
    before = (root / "CURRENT.json").read_bytes()
    with pytest.raises(RuntimeError, match="Injected failure"):
        run(root, retries=0, fail_stage="after-load")
    assert (root / "CURRENT.json").read_bytes() == before
    statuses = [json.loads(path.read_text()) for path in (root / "runs").glob("*/run.json")]
    assert any(status["status"] == "failed" for status in statuses)


def test_rejection_gate_preserves_current(built):
    root, _, _ = built
    before = current_release(root)
    with pytest.raises(ValueError, match="Rejection rate"):
        run(root, max_reject_rate=0.01, retries=0)
    assert current_release(root) == before


def test_replay_late_arrival_full_refresh_and_rollback(built):
    root, fixtures, first = built
    # Replaying the same source must not create another sale.
    replay = run(root, retries=0)
    assert replay["row_counts"] == first["row_counts"]
    ingest(root, fixtures[1])
    late = run(root, lookback_days=0, retries=0)
    assert late["row_counts"]["fct_invoice_line"] == 14
    assert query(root, "SELECT sum(net_amount) FROM fct_invoice_line") == [(Decimal("43.97"),)]
    assert late["watermark"] == first["watermark"]
    # A January backfill includes whole overlapping files and does not delete February.
    backfill = run(root, start=datetime(2024, 1, 1), end=datetime(2024, 1, 5), retries=0)
    assert backfill["row_counts"] == late["row_counts"]
    assert len(backfill["selected_manifests"]) == 2
    full = run(root, full_refresh=True, retries=0)
    assert full["row_counts"] == late["row_counts"]
    rollback(root, first["release_id"])
    assert query(root, "SELECT sum(net_amount) FROM fct_invoice_line") == [(Decimal("33.99"),)]
    # Raw remains available after rollback; normal ingestion catches up again.
    caught_up = run(root, retries=0)
    assert caught_up["row_counts"] == late["row_counts"]


def test_unseen_old_manifest_bypasses_watermark():
    old = {"source_sha256": "new", "min_event_at": "2020-01-01T00:00:00", "max_event_at": "2020-01-01T00:00:00"}
    assert select_manifest(old, set(), datetime(2024, 1, 1), 0, None, None)
    assert not select_manifest(old, {"new"}, datetime(2024, 1, 1), 7, None, None)


def test_transient_retry_and_permanent_failure(tmp_path, monkeypatch):
    calls = []

    def fake(root, candidate, **kwargs):
        calls.append(candidate)
        if len(calls) == 1:
            raise OSError("transient filesystem failure")
        candidate.mkdir(parents=True)
        return {"files": {}, "row_counts": {}}

    monkeypatch.setattr(pipeline, "_candidate", fake)
    monkeypatch.setattr(pipeline.time, "sleep", lambda seconds: None)
    result = run(tmp_path, retries=1)
    assert len(calls) == 2
    status = json.loads((tmp_path / "runs" / result["run_id"] / "run.json").read_text())
    assert status["status"] == "succeeded"
    assert [a["status"] for a in status["attempts"]] == ["failed", "succeeded"]


def test_writer_lock_rejects_second_holder(tmp_path):
    with writer_lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another warehouse writer"):
            with writer_lock(tmp_path):
                pass


@pytest.mark.parametrize("kwargs", [
    {"lookback_days": -1}, {"max_reject_rate": 1.1}, {"retries": -1},
    {"start": datetime(2024, 1, 1)},
    {"start": datetime(2024, 2, 1), "end": datetime(2024, 1, 1)},
    {"full_refresh": True, "start": datetime(2024, 1, 1), "end": datetime(2024, 2, 1)},
])
def test_invalid_run_options(tmp_path, kwargs):
    with pytest.raises(ValueError):
        run(tmp_path, **kwargs)


def test_before_publish_failure_preserves_release(built):
    root, _, _ = built
    before = (root / "CURRENT.json").read_bytes()
    with pytest.raises(RuntimeError, match="before publish"):
        run(root, retries=0, fail_stage="before-publish")
    assert (root / "CURRENT.json").read_bytes() == before
    verify_release(current_release(root))


def test_real_dbt_quality_failure_is_not_published(built, tmp_path):
    import shutil

    root, _, _ = built
    project = tmp_path / "dbt"
    shutil.copytree(pipeline.DEFAULT_DBT_PROJECT, project)
    (project / "tests" / "deliberate_failure.sql").write_text("select 1 as must_fail\n")
    before = current_release(root)
    with pytest.raises(RuntimeError, match="dbt build failed"):
        run(root, project=project, retries=2)
    assert current_release(root) == before
    statuses = [json.loads(path.read_text()) for path in (root / "runs").glob("*/run.json")]
    failure = next(s for s in statuses if any("dbt build failed" in a.get("error", "") for a in s["attempts"]))
    assert len(failure["attempts"]) == 1  # deterministic quality failures are not retried


def test_aware_backfill_bounds_rejected(tmp_path):
    from datetime import timezone

    with pytest.raises(ValueError, match="timezone-naive"):
        run(tmp_path, start=datetime(2024, 1, 1, tzinfo=timezone.utc), end=datetime(2024, 2, 1))


def test_rollback_rejects_corrupt_export(built, tmp_path):
    import shutil

    root, _, _ = built
    release = current_release(root)
    isolated = tmp_path / "isolated"
    destination = isolated / "releases" / release.name
    shutil.copytree(release, destination)
    (destination / "parquet" / "dim_date.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum mismatch"):
        rollback(isolated, release.name)
    assert not (isolated / "CURRENT.json").exists()


def isolated_published_root(built, destination):
    """Copy only raw evidence and the current release for destructive tests."""
    import shutil

    source, _, _ = built
    release = current_release(source)
    shutil.copytree(source / "raw", destination / "raw")
    shutil.copytree(release, destination / "releases" / release.name)
    shutil.copyfile(source / "CURRENT.json", destination / "CURRENT.json")
    return destination


def test_incremental_run_rejects_corrupted_published_database(built, tmp_path):
    root = isolated_published_root(built, tmp_path / "isolated")
    expected_sales = query(root, "SELECT sum(net_amount) FROM fct_invoice_line")
    release = current_release(root)
    con = duckdb.connect(str(release / "warehouse.duckdb"))
    try:
        con.execute("""UPDATE source_lines SET net_amount = net_amount + 100
                       WHERE source_row_id = (SELECT source_row_id FROM source_lines
                                              WHERE rejection_reason IS NULL LIMIT 1)""")
        con.execute("CHECKPOINT")
    finally:
        con.close()
    before = (root / "CURRENT.json").read_bytes()
    releases_before = set((root / "releases").iterdir())
    with pytest.raises(ValueError, match="Release checksum mismatch: warehouse.duckdb"):
        run(root, retries=0)
    assert (root / "CURRENT.json").read_bytes() == before
    assert set((root / "releases").iterdir()) == releases_before
    status = json.loads(next((root / "runs").glob("*/run.json")).read_text())
    assert status["status"] == "failed"
    # Explicit recovery uses verified raw, never the corrupted database.
    recovered = run(root, full_refresh=True, retries=0)
    assert recovered["release_id"] != release.name
    assert query(root, "SELECT sum(net_amount) FROM fct_invoice_line") == expected_sales


def test_incremental_run_checks_copied_database(built, tmp_path, monkeypatch):
    root = isolated_published_root(built, tmp_path / "isolated")
    original_copy = pipeline.shutil.copyfile

    def corrupt_copy(source, destination, **kwargs):
        result = original_copy(source, destination, **kwargs)
        if str(destination).endswith("warehouse.duckdb"):
            with open(destination, "ab") as stream:
                stream.write(b"copy-corruption")
        return result

    monkeypatch.setattr(pipeline.shutil, "copyfile", corrupt_copy)
    before = (root / "CURRENT.json").read_bytes()
    with pytest.raises(ValueError, match="Copied database checksum mismatch"):
        run(root, retries=0)
    assert (root / "CURRENT.json").read_bytes() == before
    verify_release(current_release(root))


def test_timestamp_domain_quarantines_unjoinable_rows(tmp_path):
    import csv

    from retail_warehouse.fixtures import HEADERS, ROWS

    source = tmp_path / "date-boundaries.csv"
    with source.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(HEADERS)
        for date in ["1899-12-31", "1900-01-01", "9999-12-30 23:59:59", "9999-12-31"]:
            row = ROWS[0].copy()
            row[4] = date
            writer.writerow(row)
    root = tmp_path / "warehouse"
    manifest = ingest(root, source)
    assert manifest["rejected_count"] == 2
    result = run(root, max_reject_rate=0.5, retries=0)
    assert result["row_counts"]["fct_invoice_line"] == 2
    assert result["row_counts"]["quarantine"] == 2
