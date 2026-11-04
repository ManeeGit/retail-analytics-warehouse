# Verification record

Verified in the cloud Linux workspace on 2026-11-04 using Python 3.12.14.
This records local results, not a claim that remote GitHub Actions has run.

| Check | Result |
|---|---|
| `ruff check .` | Passed |
| `python -m compileall -q src tests` | Passed |
| `pytest -q` | 44 passed, 106.43 seconds in this environment |
| `make demo` | Passed; final fact count 14 and net sales 43.97 |
| `retail verify` | Passed for the demo's current release |
| Real dbt build | 8 models succeeded; 21 data tests passed |

The pytest suite includes real dbt builds for initial publication, replay, late
arrivals, backfill, full refresh, rollback and catch-up. It verifies an injected
failure before publication and an intentionally failing dbt data test preserve
the current release. Other cases cover deterministic fixtures, duplicate physical
row preservation, CSV/XLSX ingestion, invalid data, decimal rounding, synthetic
SCD2 joins, unknown customers, source/release tampering, the rejection threshold,
retry behavior, writer exclusion and invalid options. An independent review
identified two additional cases, now fixed and covered: a modified published
database must fail verification before incremental reuse (and the copied database
is checked independently), and dates outside the SCD2 validity domain must be
quarantined. Recovery from trusted raw through full refresh is verified too.
The three focused integrity/date integration regressions also passed separately. Only the transient-retry
unit test substitutes a candidate builder; integration tests execute actual dbt.

Not verified here: execution on a physical 8 GB Mac, the complete external UCI
dataset, Python 3.11, power-loss durability, network filesystems, distributed
writers, or a published GitHub Actions run. The CI definition targets Python 3.11
and 3.12; inspect its remote result after publishing. The dependency lock records
the resolved package versions used by these checks.
