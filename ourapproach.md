# Our Approach: Data Lake Partitioning & File-Sizing Strategy

## 1. Why we are following this approach

The project requirement asks for a **time-partitioned data lake** that must:
- Demonstrate **pruning** (queries skip irrelevant partitions)
- Avoid **excessive small files** (file-size degradation)
- Document **trade-offs** for common query patterns
- Be **runnable locally** with clear documentation
- Expose **failures and data-quality issues**
- Produce **measurable outputs**

We chose a **modular, Python-based local pipeline** because:

| Decision | Rationale |
|----------|-----------|
| **Python + Pandas + PyArrow** | Widely understood, no cloud credentials needed, runs locally, excellent Parquet support |
| **Time-partitioned directory structure** (`year=/month=/day=`) | Enables partition pruning at query time; aligns with data lake best practices |
| **Parquet output format** | Columnar, compressed, schema-enforced; ideal for analytical queries and small-file avoidance |
| **Quarantine/reject path** | Keeps bad data traceable without corrupting the target lake |
| **Run ID + idempotent loads** | Makes reruns safe; prevents duplicate records on reprocessing |
| **Fixed random seed for synthetic data** | Ensures reproducibility across runs and reviewers |
| **Comprehensive test suite** | Validates correctness for happy path, edge cases, malformed input, duplicates, nulls |

---

## 2. How we are implementing

### Phase A — Foundation (Steps 1–2)

1. **README & Data Dictionary** — Document architecture, assumptions, naming conventions (`snake_case`, ISO-8601 timestamps), and setup instructions.
2. **Architecture Diagram / Flow Description** — Visual or text-based flow: Source → Ingestion → Validation → Transformation → Target → Metrics.
3. **Core Pipeline Module** — Configurable paths/parameters (source path, target path, partition strategy, file-size threshold). Implemented as clear functions/modules, not a monolithic script.

### Phase B — Data Quality (Steps 3–4)

1. **Schema Validation** — Check primary keys, referential integrity, nullability, data types.
2. **Business-Rule Checks** — Domain-specific validations (e.g., amount > 0, valid status values).
3. **Duplicate Handling** — Detect duplicate keys/events; route duplicates to quarantine.
4. **Quarantine Path** — Invalid records written separately with `source_file`, `run_id`, and rejection reason.
5. **Logging & Metrics** — Emit run ID, start/end time, input/output counts, rejected count, failure reason.

### Phase C — Reliability (Steps 5–6)

1. **Automated Tests** — Cover happy path, empty input, malformed records, duplicates, nulls, and at least two domain-specific edge cases.
2. **Idempotent Load** — Same input rerun does not create unintended duplicates. Documented retry, restart, and backfill behavior.

### Phase D — Data & Verification (Steps 7–8)

1. **Sample Data Generator** — Synthetic data with fixed random seed; normal data, boundary values, duplicates, missing values, late/out-of-order data, deliberately invalid records. Expected-answer labels kept separate from input.
2. **Reconciliation** — Source-to-target counts and key aggregates compared; differences explained.

### Phase E — Performance & Final Delivery (Steps 9–10)

1. **Performance Measurement** — Simple timing/metrics for sample vs. larger run.
2. **Optimization** — One reasonable optimization documented (e.g., coalescing small files before write, or choosing partition granularity to balance pruning vs. file count).
3. **Final Submission** — Source code, tests, sample input/output, README, data dictionary, design/trade-off note.

---

## 3. What we are doing in this project (summary)

| Component | Implementation Detail |
|-----------|----------------------|
| **Partitioning** | Time-based directory partitioning (`event_date=YYYY-MM-DD`) so queries prune irrelevant partitions |
| **File Sizing** | Coalesce output to target file-size threshold; avoid writing one tiny file per micro-batch |
| **Validation** | Schema + business-rule checks at ingestion; rejects go to quarantine with full traceability |
| **Idempotency** | Run ID tracked; reruns of same input are safe and do not duplicate records |
| **Testing** | Pytest-based suite covering normal, edge, and failure scenarios |
| **Observability** | Structured logging with run status, row counts, rejects, and failure reasons |
| **Reproducibility** | Fixed random seed; synthetic data; clean-environment setup instructions |

---

## 4. Trade-offs we considered

| Choice | Alternative considered | Why we chose this |
|--------|----------------------|-------------------|
| Daily partitioning | Hourly / Monthly | Daily balances pruning benefit vs. too many small directories/files |
| Parquet output | CSV / JSON | Parquet is columnar, compressed, schema-enforced, and avoids small-file overhead |
| Local Python pipeline | Cloud (Spark/Athena/Glue) | Runs locally without credentials; easier to review and test within 72 hours |
| Pandas for transformation | PySpark / SQL | Sufficient for sample-to-medium volumes; simpler dependency tree |
| Quarantine as separate files | Inline error column | Keeps target clean; bad data is isolated and traceable |
