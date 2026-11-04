# Learning guide

Work through these exercises with a small deterministic synthetic dataset first.
The aim is to explain why each boundary exists, not merely to make commands turn
green. Use the runbook for the supported command syntax.

## 1. Follow one sale

Locate a source transaction, its manifest, accepted raw row, dimensional fact,
and contribution to an aggregate. Write down its source file hash, sheet, row
position, source-row identifier, quantity, unit price, and signed fixed-precision amount.

**Explain:** Why is a filename not a sufficient input identifier? Why can the
same business sale still be represented twice if it arrives in two different
source files?

## 2. Separate missing customer from invalid sale

Compare a valid transaction with no customer ID against a transaction with an
invalid date or price. The former belongs in analytics under an unknown customer;
the latter belongs in quarantine. Inspect the recorded rejection reason and
account for both accepted and rejected rows.

**Explain:** Which fields are required to calculate a sale? Which fields enrich
its analysis? Why would dropping all anonymous transactions bias revenue?

## 3. Reason about money and returns

Follow a purchase and a return/cancellation through the raw and fact layers.
Check signed quantities and amounts rather than applying absolute values. Inspect
how four-decimal unit prices become two-decimal line amounts using half-up
rounding after multiplication. Negative prices are invalid, but negative quantities need not be.

**Explain:** Why should returns reduce net sales? What information is lost when
all negative rows are removed? Why is binary floating point an awkward default
for exact cent reconciliation?

## 4. Reprocess without multiplying rows

Run the same ingestion and warehouse workflow again. Compare raw source-row
counts and signed totals rather than expecting the new run ID or release name to
be identical. Inspect the manifests selected for the second candidate.

Then use the supported lookback or explicit backfill option. Confirm that
reselected rows merge by source identity rather than append again.

**Explain:** What does idempotency mean at the row level? Why are a lookback and a
backfill useful even when newly discovered manifests are always eligible in a
normal, unbounded run? What
happens if a corrected file has a different SHA-256?

## 5. Understand customer history

Inspect a synthetic customer with multiple dimension versions and find sales on
either side of a version change. Check the effective-date boundaries and which
version each fact references. Check the unknown-customer member separately.

**Explain:** Why is joining every historical sale to today's customer attributes
not the same question as an as-of join? Why must validity ranges avoid ambiguous
overlap? Which parts of this history are synthetic rather than present in UCI?

## 6. Inspect a quality gate

Read the Python tests and dbt tests. Identify which enforce row identities,
relationships, mandatory fields, and aggregate consistency. In a disposable copy,
introduce a condition a test rejects and observe the candidate failure.

Record the current published release before and after the failed build.

**Explain:** Why should a model that runs successfully still fail publication?
What errors would the present tests not detect?

## 7. Publish and roll back

Create two successful releases and inspect the `CURRENT.json` pointer. Roll back to
the first release using the supported command. Verify that consumers see the
older release while manifests, raw inputs, and run history remain available.

**Explain:** What is the difference between changing the published view and
reversing ingestion? Why might the next build reintroduce data from the newer
release? What would you need to recover from disk loss?

## 8. Try the optional real dataset

Only after the synthetic exercises, use the supported UCI ingestion path. Review
its provenance and license, measure input size, and start with realistic resource
expectations. Inspect schema differences, missing customers, cancellations,
quarantine, and signed totals. Do not infer genuine customer attribute history
from transaction rows alone.

## Portfolio discussion checklist

Be ready to describe:

- the grain of a raw row and a fact row;
- content identity versus business identity;
- accepted versus quarantined records;
- decimal monetary conversion and signed returns;
- incremental raw selection versus fully rebuilt marts;
- synthetic SCD2 validity intervals and unknown customers;
- the candidate/test/publication boundary;
- retry behavior, local locking, and pointer-only rollback;
- what this local project does not establish about production scale.

A useful extension is a written design for one missing capability, such as
business-key duplicate detection or incremental mart repair. State its invariants
and failure cases before implementing it. Do not add infrastructure solely to
make the architecture diagram larger.
