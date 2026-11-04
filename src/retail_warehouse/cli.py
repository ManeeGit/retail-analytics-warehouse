"""Command line entry point. No cloud accounts, scheduler, or daemon required."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb

from .fixtures import generate
from .ingest import ingest
from .pipeline import DEFAULT_DBT_PROJECT, rollback, run, verify_release
from .storage import current_release


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--root", type=Path, default=Path("data"), help="Warehouse state directory (default: data)")
    sub = result.add_subparsers(dest="command", required=True)
    fixture = sub.add_parser("fixture", help="Write deterministic initial and late-arrival CSVs")
    fixture.add_argument("--output", type=Path, default=Path("data/fixtures"))
    load = sub.add_parser("ingest", help="Snapshot a CSV/XLSX and write immutable bronze Parquet")
    load.add_argument("source", type=Path)
    load.add_argument("--provenance", default="user-provided")
    build = sub.add_parser("run", help="Build, test, export and publish a complete release")
    build.add_argument("--full-refresh", action="store_true")
    build.add_argument("--lookback-days", type=int, default=7)
    build.add_argument("--from", dest="start", type=datetime.fromisoformat)
    build.add_argument("--to", dest="end", type=datetime.fromisoformat, help="Exclusive backfill end")
    build.add_argument("--max-reject-rate", type=float, default=0.30)
    build.add_argument("--retries", type=int, default=2)
    build.add_argument("--dbt-project", type=Path, default=DEFAULT_DBT_PROJECT)
    build.add_argument("--fail-stage", choices=["after-load", "before-publish"], help="Deliberate incident drill")
    sub.add_parser("status", help="Show published release and metrics")
    sub.add_parser("verify", help="Verify all published data file checksums")
    restore = sub.add_parser("rollback", help="Atomically point readers at a previous verified release")
    restore.add_argument("release_id")
    query = sub.add_parser("query", help="Execute read-only SQL against one immutable release")
    query.add_argument("sql")
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "fixture":
            output = {"files": [str(path) for path in generate(args.output)]}
        elif args.command == "ingest":
            output = ingest(args.root, args.source, provenance=args.provenance)
        elif args.command == "run":
            output = run(args.root, full_refresh=args.full_refresh, lookback_days=args.lookback_days,
                         start=args.start, end=args.end, max_reject_rate=args.max_reject_rate,
                         retries=args.retries, project=args.dbt_project, fail_stage=args.fail_stage)
        elif args.command == "rollback":
            output = rollback(args.root, args.release_id)
        else:
            release = current_release(args.root)
            if release is None:
                raise ValueError("No published release")
            if args.command == "status":
                output = json.loads((release / "SUCCESS.json").read_text())
            elif args.command == "verify":
                output = {"verified": verify_release(release)["release_id"]}
            else:
                con = duckdb.connect(str(release / "warehouse.duckdb"), read_only=True)
                try:
                    # Prevent COPY/file operations and extensions in the convenience reader.
                    con.execute("SET enable_external_access=false")
                    rows = con.execute(args.sql)
                    output = {"columns": [item[0] for item in rows.description], "rows": rows.fetchall()}
                finally:
                    con.close()
        print(json.dumps(output, indent=2, default=str))
        return 0
    except (ValueError, RuntimeError, OSError, duckdb.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
