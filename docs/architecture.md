# Architecture and correctness boundaries

This is a local, learning-oriented retail warehouse built around Python, DuckDB,
Parquet, dbt, and pytest. It demonstrates data engineering correctness on one
machine, not distributed throughput or production readiness. An 8 GB Mac is a
reasonable target for small synthetic exercises; larger input needs measurement,
not an assumption that it will fit.

## Data flow

```text
Synthetic or optional UCI source
  -> immutable ingestion manifests and source snapshots
  -> validated staging rows + quarantine
  -> incremental raw candidate
  -> complete dbt dimensional rebuild and quality tests
  -> immutable versioned release
  -> atomic CURRENT.json pointer
```

The important boundary is between *building a candidate* and *publishing a
release*. A candidate can fail without replacing the last successfully published
warehouse.

## Identity and validation

A source row is identified using the source file SHA-256, sheet, and row position.
That identifier is stable for a repeat ingestion of the same bytes and position.
It is not a universal business transaction key: exporting the same sale in a
byte-different file can create a different source identity. Recognizing duplicate
business events across distinct exports is a separate problem.

Immutable manifests make source selection inspectable. They record the input
rather than using a mutable filename as proof of what was processed. File hashes
identify contents; they do not prove that a source was accurate or trustworthy.

Validation separates accepted rows from quarantine. Missing invoice or stock codes, missing or invalid transaction dates, invalid
quantity or price values, and negative prices are rejected. Zero quantities,
non-integer quantities, and values outside the supported numeric bounds are
invalid as well. Timestamps must be in the half-open source-local range
`[1900-01-01, 9999-12-31)` shared by customer-history intervals; out-of-range
values are quarantined before dimensional joins. A missing customer is different: it is allowed and maps to
an unknown customer rather than discarding the sale. A cancellation or a negative
quantity is a signed transaction, not automatically bad data. Monetary conversion
uses decimal arithmetic: unit price is `DECIMAL(18,4)` and signed line amount is
`DECIMAL(18,2)`, rounded half up after multiplication. Amounts are stored as fixed
precision decimals, not integer cents. Binary floating-point artifacts do not
become the accounting rule.

Quarantine is an observable outcome. It must be reviewed alongside accepted row
counts, not treated as a successful silent deletion of inconvenient input.

## Incremental raw data, full analytical rebuild

By default the raw candidate selects manifests not already loaded, plus eligible
manifests in a configured lookback window. An explicit bounded backfill instead
selects complete manifests overlapping that interval; it does not add unrelated
new manifests or filter individual rows out of a selected file. Previously loaded
rows are retained. All source rows, including rejected rows, are merged by stable
source-row identity with insert-on-conflict-do-nothing semantics. This supports reprocessing overlapping
inputs without appending the same identified row again.

The dbt dimensional models are rebuilt from the resulting raw candidate. This is
an intentional tradeoff: incremental ingestion is easier to reason about while
complete marts avoid leaving affected aggregates or dimension joins stale. It
also means the project does **not** demonstrate an end-to-end incremental mart
strategy. Rebuilding a large warehouse will eventually become too expensive.

Synthetic customer history supplies the slowly changing dimension exercise. It
allows queries to associate sales with the customer attributes effective at the
transaction time. Customer `101` changes from `standard` to `loyal` on
2024-02-01; the other generated customer records are static. Country remains an
observation on the fact, not a historical customer attribute. The date dimension
contains only dates present in accepted rows, not a gap-free calendar. Do not mistake that generated history for historical customer
attributes recovered from the UCI transaction file.

## Publication, retries, and rollback

A run ID groups one or more numbered attempts and makes failures inspectable.
Only operating-system errors and subprocess timeouts are retried automatically;
validation and dbt quality failures require investigation. A process lock prevents
concurrent local writers from publishing conflicting candidates. This is a
single-machine coordination mechanism, not a distributed lock service.

Before an incremental build reuses a published release, every recorded data-file
checksum is verified. The copied database is checked against the verified
expected hash before it is opened, so source corruption cannot be republished
under new checksums. A full refresh does not reuse the previous database.

The build and dbt tests complete before publication. A successful candidate is
placed in its versioned release directory and an atomic replacement updates the
`CURRENT.json` pointer. Consumers should resolve `CURRENT.json` once for an operation and
read that release consistently, rather than combine files from changing releases.

Rollback changes the published pointer to an earlier release. It does not erase
raw ingestion, manifests, or run history. The next build can therefore include
newer ingested data again. Rolling back publication is not deleting a source,
reverting all state, or restoring a backup.

Atomic pointer replacement protects the publication boundary. It does not make
several files transactional while they are being built, protect against disk
loss, or guarantee durability under every abrupt system or filesystem failure.
Immutability is an application convention, not an operating-system permission
barrier against manual edits. Keep the release and pointer on the intended local
filesystem and retain backups
for data that cannot be regenerated.

## Quality gates and limits

Python tests exercise ingestion and orchestration behavior. dbt tests gate model
quality before the release becomes current. Neither can establish that external
source data is complete or economically correct. Inspect quarantine, reconcile
counts and signed totals, and understand the input's definitions as well.

The system is intentionally local: no distributed scheduler, high-availability
service, multi-user access control, or production capacity guarantee is implied.
