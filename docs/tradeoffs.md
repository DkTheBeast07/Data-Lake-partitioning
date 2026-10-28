# Design Trade-offs

## Partitioning Granularity

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| Hourly | Best pruning for time-range queries | Many directories, more small files | ❌ |
| Daily | Good balance of pruning vs. file count | Less granular pruning for sub-day queries | ✅ Daily |
| Monthly | Fewest directories, largest files | Poor pruning for recent data | ❌ |

**Decision: Daily partitioning.** Provides good query pruning for most analytical workloads while keeping directory/file counts manageable.

## Output Format

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| Parquet | Columnar, compressed, schema-enforced, good for analytics | Binary format, not human-readable | ✅ Parquet |
| CSV | Human-readable, universally supported | No compression, no schema enforcement, larger files | ❌ |
| JSON | Human-readable, flexible schema | Verbose, no compression, larger files | ❌ |

**Decision: Parquet.** Best for analytical queries on a data lake. Columnar storage enables efficient scanning and compression reduces storage costs.

## Processing Engine

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| Pandas + PyArrow | Simple, runs locally, no cluster setup | Limited to single machine memory | ✅ Pandas |
| PySpark | Distributed, handles large volumes | Complex setup, overkill for this project | ❌ |
| SQL / dbt | Good for transformations | Requires database infrastructure | ❌ |

**Decision: Pandas + PyArrow.** Sufficient for the project scope (100K+ records), runs locally, easy to understand and test.

## Idempotency Approach

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| Input hashing + state tracking | Simple, reliable, no external dependencies | State stored locally | ✅ Input hashing |
| Database transaction log | Durable, queryable | Requires database infrastructure | ❌ |
| File-based watermark | Simple | Less robust for concurrent runs | ❌ |

**Decision: Input hashing + local state tracking.** Simple and effective for a single-node pipeline. State stored in `data/state/`.

## Quarantine Strategy

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| Separate files | Clean target, easy to inspect rejects | Additional I/O | ✅ Separate files |
| Inline error column | Single output, simpler pipeline | Target contains bad data | ❌ |
| Dead letter queue | Enterprise-grade | Overkill, requires messaging infra | ❌ |

**Decision: Separate quarantine files.** Keeps the target data lake clean and makes it easy to inspect and fix rejected records.

## Validation Location

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| Pre-load validation | Catches issues early, clean target | Extra pass over data | ✅ Pre-load |
| Post-load validation | Simpler pipeline | Bad data enters target | ❌ |
| No validation | Fastest | No data quality guarantees | ❌ |

**Decision: Pre-load validation.** Schema and business rules checked before writing to target, ensuring data quality in the lake.

## Known Limitations

1. **Single-node**: Pandas is limited to memory of one machine. For very large datasets (>100GB), consider PySpark.
2. **Local state**: Idempotency state is stored locally. For distributed deployments, use a shared database or distributed cache.
3. **No incremental loads**: Currently processes full files. For CDC/incremental, add last-processed timestamp tracking.
4. **No schema evolution**: Schema is fixed from data_dictionary.json. For evolving schemas, add schema versioning and migration.