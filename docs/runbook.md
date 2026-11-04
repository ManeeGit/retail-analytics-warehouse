# Local runbook

Run commands from the repository checkout. Python 3.11 or newer is required.
The local writer lock uses `fcntl`, so this workflow targets macOS and Linux.
No Docker, cloud account, scheduler, or downloaded dataset is needed for the demo.

## Install and check

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install --no-deps -e .
pytest -q
retail --help
```

The editable install preserves access to the checkout's dbt project. When using a
separate project location, pass `run --dbt-project /absolute/path/to/dbt`.

The examples use `data` as the warehouse state root. The global `--root` option
must appear **before** the subcommand. The fixture command's output path is
separate: specify `--output` explicitly when changing it.

## First synthetic release

```bash
retail fixture --output data/fixtures
retail --root data ingest data/fixtures/initial.csv --provenance synthetic-demo
retail --root data run
retail --root data status
retail --root data verify
retail --root data query "SELECT count(*) AS lines, sum(net_amount) AS net_sales FROM fct_invoice_line"
retail --root data query "SELECT rejection_reason, count(*) AS rows FROM quarantine GROUP BY 1"
```

The initial fixture has 17 source rows: 13 accepted and four quarantined. Net
sales are 33.99. The duplicate-looking `10009` lines are separate physical source
rows and intentionally both remain. The `0.335` unit price multiplied by three
produces a line amount of 1.01 after half-up rounding.

The default rejection threshold is 30%, so the fixture's 4/17 rejected rows pass.
A successful run still warrants looking at quarantine. The threshold applies to
all source rows in the candidate, not only the newly selected manifests. Do not
raise it merely to hide a source-quality incident.

## Replay and late arrivals

```bash
retail --root data ingest data/fixtures/initial.csv --provenance synthetic-demo
retail --root data run
retail --root data ingest data/fixtures/late.csv --provenance synthetic-late-arrival
retail --root data run --lookback-days 7
retail --root data query "SELECT count(*) AS lines, sum(net_amount) AS net_sales FROM fct_invoice_line"
```

Reingesting identical bytes returns the verified existing manifest. Rebuilding
creates a new release, but does not duplicate the same source-row IDs. After the
late fixture, expect 14 accepted rows and net sales of 43.97. The late event is in
January even though the existing watermark is in February. A previously unloaded
manifest is eligible regardless of that watermark in a normal run.

Lookback uses the maximum accepted event timestamp in the prior warehouse, not
the wall-clock date. It reselects complete files whose maximum event time reaches
the lookback boundary.

## Explicit backfill and full rebuild

```bash
retail --root data run --from 2024-01-01 --to 2024-02-01
retail --root data run --full-refresh
```

Both backfill bounds are required; the end is exclusive. Use timezone-naive ISO
dates/timestamps consistent with source-local timestamps. Accepted invoice timestamps must be
in `[1900-01-01, 9999-12-31)`; out-of-range values go to quarantine. Backfill selects files
whose event ranges overlap the interval, then replays each selected file in full.
It retains previously loaded rows and may include rows outside the requested
interval. It is not a filtered export or a deletion/replacement window. During a
bounded backfill, unrelated newly ingested manifests wait for a normal run.

Full refresh starts an empty candidate and rebuilds from all known ingested
manifests. It cannot be combined with bounded backfill. Every successful mode
rebuilds the dbt tables and runs their quality tests.

## Inspect the published release

```text
data/
  raw/<source-sha256>/
    source.csv or source.xlsx
    bronze.parquet
    manifest.json
  runs/<run-id>/run.json
  runs/<run-id>/attempt-<n>/     # failed candidates remain here
  releases/<release-id>/
    warehouse.duckdb
    parquet/*.parquet
    dbt/build.log
    dbt/target/                 # dbt manifests and results
    dbt/logs/
    SUCCESS.json
  CURRENT.json
  rollbacks/<event-id>.json
```

Successful attempt directories are moved into `releases`; they do not remain
under `runs`. `SUCCESS.json` records selected manifests, row counts, rejection
metrics, watermark, build options, and checksums of the database and Parquet
exports. `verify` checks those data files, not every dbt log or artifact. Keep
published releases immutable: use `retail query` for read-only SQL and rebuild
rather than editing a released database.

## Failure and rollback drill

First capture the current release ID from `retail --root data status`, then:

```bash
retail --root data run --fail-stage after-load
retail --root data status
retail --root data run --fail-stage before-publish
retail --root data status
retail --root data verify
```

Both injected runs should exit nonzero and leave the current release ID unchanged.
Inspect their `runs/<run-id>/run.json` and retained attempt directories. The
before-publish drill executes dbt first, so it takes longer.

To switch to an earlier successful release, copy its actual ID into:

```bash
retail --root data rollback REPLACE_WITH_EXISTING_RELEASE_ID
retail --root data status
retail --root data verify
```

Rollback verifies the target's recorded data checksums before updating the pointer
and writes a separate audit event. Raw ingestion and run history remain. A later
normal run may pick up inputs newer than the rolled-back release again.

## Troubleshooting

- **No published release:** Ingest an input and complete `run`. Ingestion alone
  does not publish a queryable warehouse.
- **Another warehouse writer is running:** Let the active ingestion/build/rollback
  finish and rerun. The OS releases the advisory lock when its process exits;
  deleting the lock file is not the recovery procedure.
- **Rejection threshold exceeded:** Inspect raw manifest counts and the candidate
  database's `source_lines` rejection reasons. Fix the source understanding or
  validation problem before deciding whether a threshold adjustment is justified.
- **dbt build failed:** Open the failed attempt's `dbt/build.log`, then dbt logs and
  `target/run_results.json` if available. Correct the issue and start a new run.
  Quality failures do not receive automatic retries.
- **Checksum mismatch:** Stop using the affected release or raw snapshot. Preserve
  evidence and restore trusted copies or regenerate synthetic state in a separate
  root. Incremental runs verify the current release and its database copy before
  reuse. A full refresh can rebuild from verified raw without reusing that
  database. Do not edit checksums to make verification pass.
- **Timeout or transient filesystem error:** `run --retries 2` permits two retries
  after the initial attempt, using fresh candidate directories. The allowed range
  is zero through five. Inspect all attempts if the run fails.
- **Slow or memory-limited run:** Start with synthetic data, close competing apps,
  and check available disk. Ingestion uses batches of 5,000, a 512 MB DuckDB memory
  limit, and two threads; builds/dbt use a 1 GB DuckDB limit and two threads. These
  are engine settings, not a cap on the entire Python/dbt process tree. Every
  release keeps a database and Parquet exports, so retained releases consume disk.

## Optional UCI source

The compatible schema is UCI **Online Retail**, not an arbitrary similarly named
retail dataset: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`,
`UnitPrice`, `CustomerID`, and `Country`. Obtain its CSV/XLSX from the UCI dataset
page and review its license and attribution requirements. All worksheets in an
XLSX must have the required unique headers. There is no automatic downloader in
the CLI.

For isolation from the teaching fixture, use a separate root:

```bash
retail --root data-uci ingest '/path/to/Online Retail.xlsx' --provenance 'UCI Online Retail'
retail --root data-uci run
retail --root data-uci status
retail --root data-uci verify
```

Real data processing may take substantially longer. Review quarantine and signed
sales, and remember that the customer segment history is a synthetic teaching
rule, not UCI-provided CRM history. Do not combine fixture and real source files
in the same root unless that mixed dataset is intentional.
