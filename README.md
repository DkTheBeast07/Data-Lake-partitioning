# Data Lake Partitioning & File-Sizing Strategy

## Overview

This project implements a **time-partitioned data lake** pipeline that demonstrates:
- **Partition pruning** — time-based directory structure enables queries to skip irrelevant partitions
- **File sizing discipline** — coalesced output files avoid the small-file problem
- **Data quality** — schema validation, business rules, and quarantine for invalid records
- **Idempotency** — safe reruns without duplicate records
- **Observability** — run ID, metrics, logging, and reconciliation reports

## Project Structure

```
yotta-by-cline/
├── data/                       # Input and output data
│   ├── input/                  # Source data (sample and generated)
│   ├── output/                 # Partitioned target data lake
│   └── quarantine/             # Invalid/rejected records
├── docs/                       # Architecture and design documents
│   ├── architecture.md
│   └── tradeoffs.md
├── scripts/                    # Utility scripts
│   └── reconcile.py            # Output reconciliation
├── src/                        # Source code
│   ├── __init__.py
│   ├── pipeline.py             # Core ingestion pipeline
│   ├── validation.py           # Schema and business rule validation
│   ├── metrics.py              # Logging and operational metrics
│   └── backfill.py             # Idempotent load and retry logic
├── tests/                      # Automated test suite
│   ├── __init__.py
│   ├── test_pipeline.py
│   └── test_validation.py
├── data_generator.py           # Reproducible synthetic data generator
├── data_dictionary.json        # Schema and data dictionary
├── requirements.txt            # Python dependencies
├── project.md                  # Project overview and requirements
├── ourapproach.md              # Implementation approach and rationale
└── .gitignore                  # Git ignore rules
```

## Quick Start

### 1. Create and activate virtual environment

```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate sample data

```bash
python data_generator.py --records 1000 --output data/input/sample.csv
```

### 4. Run the pipeline

```bash
python -m src.pipeline --input data/input/sample.csv --output data/output --partition daily
```

### 5. Run tests

```bash
pytest tests/ -v
```

### 6. Reconcile output

```bash
python scripts/reconcile.py --input data/input/sample.csv --output data/output
```

## Architecture

The pipeline follows this flow:

```
Source Data → Ingestion → Validation → Transformation → Target (Partitioned)
                                              ↓
                                        Quarantine (Invalid)
                                              ↓
                                        Metrics & Logging
```

### Partitioning Strategy

Data is partitioned by event date:
```
data/output/
  year=2024/
    month=01/
      day=01/
        part-00000.parquet
      day=02/
        part-00000.parquet
    month=02/
      ...
  year=2025/
    ...
```

### File Sizing

Output files are coalesced to target approximately 128MB per file (configurable) to avoid excessive small files while maintaining good parallelism.

## Data Quality

- **Schema validation**: Checks data types, nullability, and required fields
- **Business rules**: Validates domain constraints (e.g., positive amounts, valid statuses)
- **Duplicate detection**: Identifies duplicate event keys
- **Quarantine**: Invalid records are isolated with rejection reason and source traceability
- **Referential integrity**: Validates foreign key relationships

## Idempotency

- Each run generates a unique `run_id`
- Rerunning the same input does not create duplicate records
- Run status, row counts, and rejects are recorded per run
- Supports retry, restart, and backfill scenarios

## Testing

The test suite covers:
- Happy path (valid input → correct output)
- Empty input
- Malformed records
- Duplicate keys
- Null values
- Domain-specific edge cases (late data, invalid references)

## Output

- **Target**: Partitioned Parquet files in `data/output/`
- **Quarantine**: Invalid records in `data/quarantine/`
- **Metrics**: Run metadata logged per execution
- **Reconciliation**: Source-to-target count comparison

## License

MIT