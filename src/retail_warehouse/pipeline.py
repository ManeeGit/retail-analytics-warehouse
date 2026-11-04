"""Build candidates privately, run quality gates, then atomically publish a release."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from .ingest import BRONZE_DDL, verify_manifest
from .storage import atomic_json, current_release, sha256, utcnow, writer_lock

EXPORT_TABLES = ["stg_lines", "quarantine", "dim_customer", "dim_product", "dim_date", "fct_invoice_line", "mart_daily_sales", "mart_customer_value"]
DEFAULT_DBT_PROJECT = Path(__file__).resolve().parents[2] / "dbt"


def execute_dbt(database: Path, project: Path, artifacts: Path) -> None:
    env = os.environ.copy()
    env.update(RETAIL_DUCKDB_PATH=str(database.resolve()), DBT_SEND_ANONYMOUS_USAGE_STATS="false", DBT_USE_COLORS="false")
    executable = Path(sys.executable).with_name("dbt")
    if not executable.is_file():
        found = shutil.which("dbt")
        if not found:
            raise RuntimeError("dbt is missing; install this project's dependencies")
        executable = Path(found)
    artifacts.mkdir(parents=True, exist_ok=True)
    with (artifacts / "build.log").open("w") as log:
        result = subprocess.run([str(executable), "build", "--project-dir", str(project.resolve()),
                                 "--profiles-dir", str(project.resolve()), "--target-path", str((artifacts / "target").resolve()),
                                 "--log-path", str((artifacts / "logs").resolve())], env=env,
                                stdout=log, stderr=subprocess.STDOUT, check=False, timeout=600)
    if result.returncode:
        raise RuntimeError(f"dbt build failed; inspect {artifacts / 'build.log'}")


def select_manifest(manifest: dict, loaded: set[str], watermark: datetime | None,
                    lookback_days: int, start: datetime | None, end: datetime | None) -> bool:
    minimum = datetime.fromisoformat(manifest["min_event_at"]) if manifest["min_event_at"] else None
    maximum = datetime.fromisoformat(manifest["max_event_at"]) if manifest["max_event_at"] else None
    if start is not None and end is not None:
        # Manifest-level replay: complete files are replayed, not individual dates.
        return minimum is not None and minimum < end and maximum >= start
    if manifest["source_sha256"] not in loaded:
        return True  # Late data is never dropped merely for being behind the watermark.
    return maximum is not None and watermark is not None and maximum >= watermark - timedelta(days=lookback_days)


def _candidate(root: Path, candidate: Path, *, full_refresh: bool, lookback_days: int,
               start: datetime | None, end: datetime | None, project: Path,
               max_reject_rate: float, fail_stage: str | None) -> dict:
    candidate.mkdir(parents=True)
    database = candidate / "warehouse.duckdb"
    previous = current_release(root)
    if previous is not None and not full_refresh:
        # A published snapshot is immutable by convention, not by OS permissions.
        # Refuse corruption before it can be laundered into a fresh valid checksum.
        previous_report = verify_release(previous)
        expected_database_hash = previous_report["files"]["warehouse.duckdb"]
        shutil.copyfile(previous / "warehouse.duckdb", database)
        # Also detect corruption or concurrent manual edits during the copy.
        if sha256(database) != expected_database_hash:
            raise ValueError("Copied database checksum mismatch; prior release was not reused")
    con = duckdb.connect(str(database))
    selected = []
    try:
        con.execute("SET memory_limit='1GB'")
        con.execute("SET threads=2")
        con.execute("CREATE TABLE IF NOT EXISTS source_lines " + BRONZE_DDL)
        con.execute("CREATE TABLE IF NOT EXISTS loaded_manifests (source_sha256 VARCHAR PRIMARY KEY, loaded_at TIMESTAMP)")
        loaded = {row[0] for row in con.execute("SELECT source_sha256 FROM loaded_manifests").fetchall()}
        watermark = con.execute("SELECT max(invoice_at) FROM source_lines WHERE rejection_reason IS NULL").fetchone()[0]
        manifests = sorted((root / "raw").glob("*/manifest.json"))
        if not manifests:
            raise ValueError("No ingested source files. Run retail fixture and retail ingest first.")
        con.execute("BEGIN TRANSACTION")
        for manifest_path in manifests:
            manifest = json.loads(manifest_path.read_text())
            if not select_manifest(manifest, loaded, watermark, lookback_days, start, end):
                continue
            manifest = verify_manifest(manifest_path.parent)
            con.execute("INSERT INTO source_lines SELECT * FROM read_parquet(?) ON CONFLICT (source_row_id) DO NOTHING", [str(manifest_path.parent / "bronze.parquet")])
            con.execute("INSERT INTO loaded_manifests VALUES (?, current_timestamp) ON CONFLICT DO NOTHING", [manifest["source_sha256"]])
            selected.append(manifest["source_sha256"])
        con.execute("COMMIT")
        total, rejected, watermark = con.execute("SELECT count(*), count(rejection_reason), max(invoice_at) FILTER (WHERE rejection_reason IS NULL) FROM source_lines").fetchone()
        if not total:
            raise ValueError("No rows selected by the requested backfill")
        reject_rate = rejected / total
        if reject_rate > max_reject_rate:
            raise ValueError(f"Rejection rate {reject_rate:.2%} exceeds {max_reject_rate:.2%}")
        if total == rejected:
            raise ValueError("All rows were rejected")
    finally:
        con.close()
    if fail_stage == "after-load":
        raise RuntimeError("Injected failure after load; prior published release is unchanged")
    execute_dbt(database, project, candidate / "dbt")
    exports = candidate / "parquet"
    exports.mkdir()
    con = duckdb.connect(str(database))
    try:
        row_counts = {}
        for table in EXPORT_TABLES:
            row_counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            con.execute(f"COPY (SELECT * FROM {table}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(exports / f"{table}.parquet")])
        con.execute("CHECKPOINT")
    finally:
        con.close()
    if fail_stage == "before-publish":
        raise RuntimeError("Injected failure before publish; prior published release is unchanged")
    report = {"built_at": utcnow(), "selected_manifests": selected, "watermark": watermark.isoformat() if watermark else None,
              "row_counts": row_counts, "rejected_count": rejected, "total_source_rows": total,
              "rejection_rate": reject_rate, "full_refresh": full_refresh,
              "lookback_days": lookback_days, "backfill_start": start.isoformat() if start else None,
              "backfill_end_exclusive": end.isoformat() if end else None,
              "files": {str(path.relative_to(candidate)): sha256(path) for path in [database, *sorted(exports.glob("*.parquet"))]}}
    return report


def run(root: Path, *, full_refresh: bool = False, lookback_days: int = 7,
        start: datetime | None = None, end: datetime | None = None,
        max_reject_rate: float = 0.30, retries: int = 2,
        project: Path = DEFAULT_DBT_PROJECT, fail_stage: str | None = None) -> dict:
    if lookback_days < 0 or not 0 <= max_reject_rate <= 1 or not 0 <= retries <= 5:
        raise ValueError("Require lookback >= 0, reject rate 0..1, retries 0..5")
    if any(bound is not None and bound.tzinfo is not None for bound in (start, end)):
        raise ValueError("Backfill bounds must be timezone-naive source-local timestamps")
    if (start is None) != (end is None) or (start and end and start >= end):
        raise ValueError("Backfill requires both start and exclusive end, with start < end")
    if full_refresh and start:
        raise ValueError("A full refresh cannot be combined with a bounded backfill")
    if not (project / "dbt_project.yml").is_file():
        raise ValueError(f"dbt project missing at {project}; run from the source checkout or pass --dbt-project")
    root = root.resolve()
    with writer_lock(root):
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:12]
        run_dir = root / "runs" / run_id
        run_dir.mkdir(parents=True)
        status = {"run_id": run_id, "status": "running", "started_at": utcnow(), "attempts": []}
        atomic_json(run_dir / "run.json", status)
        for attempt in range(1, retries + 2):
            candidate = run_dir / f"attempt-{attempt}"
            try:
                report = _candidate(root, candidate, full_refresh=full_refresh, lookback_days=lookback_days,
                                    start=start, end=end, project=project, max_reject_rate=max_reject_rate,
                                    fail_stage=fail_stage)
                release_id = f"{run_id}-a{attempt}"
                report.update(release_id=release_id, run_id=run_id)
                atomic_json(candidate / "SUCCESS.json", report)
                releases = root / "releases"
                releases.mkdir(exist_ok=True)
                candidate.rename(releases / release_id)
                # The only publish operation. Readers resolve this pointer once.
                atomic_json(root / "CURRENT.json", {"release_id": release_id, "published_at": utcnow()})
            except Exception as exc:
                status["attempts"].append({"attempt": attempt, "status": "failed", "error": str(exc), "at": utcnow()})
                retryable = isinstance(exc, (OSError, subprocess.TimeoutExpired))
                status["status"] = "retrying" if retryable and attempt <= retries else "failed"
                atomic_json(run_dir / "run.json", status)
                if status["status"] == "failed":
                    raise
                time.sleep(min(2 ** (attempt - 1), 8))
                continue
            # Publication succeeded. Do not retry a committed release for a logging error.
            status["attempts"].append({"attempt": attempt, "status": "succeeded", "at": utcnow()})
            status.update(status="succeeded", finished_at=utcnow(), release_id=release_id)
            atomic_json(run_dir / "run.json", status)
            return report
    raise AssertionError("unreachable")


def verify_release(release: Path) -> dict:
    report = json.loads((release / "SUCCESS.json").read_text())
    for name, expected in report["files"].items():
        if sha256(release / name) != expected:
            raise ValueError(f"Release checksum mismatch: {name}")
    return report


def rollback(root: Path, release_id: str) -> dict:
    if Path(release_id).name != release_id or release_id.startswith("."):
        raise ValueError("Invalid release identifier")
    with writer_lock(root):
        target = root / "releases" / release_id
        report = verify_release(target)
        old = current_release(root)
        pointer = {"release_id": release_id, "published_at": utcnow(), "rolled_back_from": old.name if old else None}
        atomic_json(root / "CURRENT.json", pointer)
        # Preserve an audit event without modifying the immutable target release.
        atomic_json(root / "rollbacks" / f"{uuid.uuid4().hex}.json", pointer)
        return report
