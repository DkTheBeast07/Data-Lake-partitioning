# Architecture

## Pipeline Flow

```
┌─────────────┐    ┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Source    │───▶│  Ingestion  │───▶│  Validation  │───▶│  Transform  │
│  (CSV/JSON) │    │             │    │              │    │             │
└─────────────┘    └─────────────┘    └──────────────┘    └──────┬──────┘
                                                                │
                                          ┌─────────────────────┴─────────────────────┐
                                          ▼                                             ▼
                                   ┌───────────────┐                           ┌───────────────┐
                                   │   Target      │                           │   Quarantine  │
                                   │ (Partitioned  │                           │  (Rejected    │
                                   │  Parquet)     │                           │   Records)    │
                                   └───────────────┘                           └───────────────┘
                                          │
                                          ▼
                                   ┌───────────────┐
                                   │   Metrics &   │
                                   │   Logging     │
                                   └───────────────┘
```

## Components

### 1. DataLakePipeline (`src/pipeline.py`)
Orchestrates the full pipeline: read → validate → transform → write. Handles CLI arguments and run lifecycle.

### 2. SchemaValidator (`src/validation.py`)
Validates data against the schema defined in `data_dictionary.json`. Checks:
- Required columns present
- Null values in required fields
- Data type correctness
- Valid values (enums)

### 3. BusinessRuleValidator (`src/validation.py`)
Applies domain-specific rules:
- Amount must be positive when present
- Status must be one of: PENDING, COMPLETED, FAILED, CANCELLED

### 4. IdempotentLoader (`src/backfill.py`)
Ensures reruns don't create duplicates by hashing input data and tracking processed runs.

### 5. RunMetrics (`src/metrics.py`)
Captures operational metrics: run ID, timestamps, input/output counts, rejected count, errors.

### 6. Data Generator (`data_generator.py`)
Creates reproducible synthetic data with configurable edge cases (invalid records, duplicates).

### 7. Reconciliation (`scripts/reconcile.py`)
Compares source input to target output and produces a reconciliation report.

## Partitioning Strategy

Data is partitioned by `event_date` using Hive-style directory naming:

```
data/output/
  year=2024/
    month=01/
      day=15/
        part-abc12345.parquet
      day=16/
        part-abc12345.parquet
    month=02/
      ...
  year=2025/
    ...
```

This enables **partition pruning** — queries filtering by date only scan relevant directories.

## File Sizing

- Format: Parquet with Snappy compression
- Target file size: ~128MB (configurable)
- Files are coalesced per partition to avoid excessive small files

## Idempotency

Each run generates a unique `run_id`. Input data is hashed, and if the same input has been processed before, the run is skipped. Run state is persisted in `data/state/`.

## Data Quality

Invalid records are isolated in `data/quarantine/` with:
- Rejection reason
- Source file reference
- Run ID
- Timestamp of rejection
- Original raw record