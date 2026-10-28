# Project: Time-Partitioned Data Lake Partitioning & File-Sizing Strategy

## 1. What is this project?

This project designs and implements a **partitioning and file-sizing strategy for a time-partitioned data lake**. It demonstrates:

- **Data pruning** — using partition keys so queries scan only relevant data
- **File-sizing discipline** — avoiding excessive small files that degrade query performance
- **Trade-off documentation** — analyzing common query patterns and their performance implications

The solution delivers a working Data Engineering pipeline that runs locally, handles ingestion → validation → transformation → publishing, exposes failures and data-quality issues, and produces measurable outputs.

**Target level:** 0–3 years of experience  
**Priority:** Correctness, clear design, maintainability, and evidence of testing over unnecessary complexity

---

## 2. What do we need to do?

Based on the project requirements, we must deliver a **working Data Engineering solution** with the following workflow:

| # | Task | Description |
|---|------|-------------|
| 1 | Define source & target schemas | Data types, keys, assumptions, nullability, uniqueness, referential integrity |
| 2 | Generate sample data | Small reproducible dataset + larger scalable generator (e.g. 100,000+ records) |
| 3 | Implement pipeline | Ingestion → Validation → Transformation → Target publishing |
| 4 | Make reruns safe | Record run status, row counts, rejects, errors |
| 5 | Add automated tests | Happy path, empty input, malformed records, duplicates, nulls, edge cases |
| 6 | Add logging & metrics | Run ID, start/end time, input/output counts, rejected count, failure reason |
| 7 | Idempotent load | Idempotency, retry, restart, backfill behavior documented |
| 8 | Reproducible sample data | Synthetic data with fixed random seed; expected-answer labels separate from input |
| 9 | Demonstrate correctness | Reconciliation queries, assertions, or validation report |
| 10 | Performance measurement | Simple performance measurement + one reasonable optimization |

---

## 3. Inputs

### Source Data
- **Format:** CSV / JSON / Parquet (configurable)
- **Grain:** One record per event/transaction with timestamp
- **Fields:** Event ID, Event Timestamp, Entity ID, Value/Amount, Status, Source File, etc.
- **Volume:** Small sample for quick review + configurable larger run (100,000+ records)

### Edge Cases to Include
- Malformed input
- Missing required fields
- Duplicate keys/events
- Unexpected schema change
- Empty batch
- Partial failure & retry
- Late-arriving / out-of-order data
- Invalid reference key
- Rerun of same input

### Configuration
- Source path
- Target path
- Partition strategy (daily/hourly/monthly)
- Target file size threshold
- Run ID

---

## 4. Outputs

### Target Data (Data Lake)
- **Partitioned by time:** `target/year=YYYY/month=MM/day=DD/` or `target/event_date=YYYY-MM-DD/`
- **File format:** Parquet (columnar, compressed)
- **File sizing:** Coalesced/sized to avoid excessive small files
- **Schema:** Deterministic snake_case column names, ISO-8601 timestamps, stable deterministic IDs

### Reject / Quarantine
- Invalid records written to a separate quarantine path
- Traceable to source via `source_file` and `run_id`
- Rejection reason recorded per record

### Operational Metrics & Logs
- Run ID, start/end time
- Input record count
- Output record count
- Rejected record count
- Failure reason (if any)
- Run status (SUCCESS / FAILED)

### Documentation
- README (architecture, assumptions, run instructions, limitations, next steps)
- Data dictionary / schema documentation
- Design & trade-off note
- Test results / execution evidence
- Reconciliation report

---

## 5. Success Criteria (Acceptance)

- Primary keys and referential integrity are checked
- Transformation rules are deterministic
- Source-to-target counts and key aggregates reconcile or explain differences
- Invalid records are traceable
- Reruns do not create unintended duplicates
- All tests pass
- Setup and run instructions work from a clean environment
