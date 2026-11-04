# Verification record

Verified in the cloud Linux workspace on 2026-11-04 using Python 3.11.17 and
3.12.14. The records below are local results. A hosted CI dependency failure
identified a lock-generation bug, corrected and retested locally as described
below; the hosted rerun must be checked separately.

| Check | Result |
|---|---|
| `ruff check .` | Passed |
| `python -m compileall -q src tests` | Passed |
| `pytest -q` on Python 3.11.17 | 44 passed, 89.96 seconds |
| `pytest -q` on Python 3.12.14 | 44 passed, 125.48 seconds |
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
dataset, power-loss durability, network filesystems, distributed
writers, or a successful hosted GitHub Actions rerun after this lock fix. The CI definition targets Python 3.11
and 3.12; inspect its remote result after publishing. The dependency lock records
the resolved package versions used by these checks.


## Dependency compatibility correction

The first hosted Python 3.11 job failed during installation because the lock had
selected NetworkX 3.7, which requires Python 3.12. The original universal lock
command had implicitly targeted the local Python 3.12 interpreter. Regeneration
now explicitly uses `--python-version 3.11`, producing NetworkX 3.6.1 for Python
versions below 3.12 and NetworkX 3.7 for 3.12 and above.

The corrected lock was installed in an isolated Python 3.11.17 environment and
the existing Python 3.12.14 environment. Both passed Ruff and all 44 tests,
including real dbt integration builds. The CI matrix uses `fail-fast: false` so
one failing interpreter no longer prevents the other result from completing.
