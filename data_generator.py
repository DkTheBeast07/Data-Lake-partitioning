"""Reproducible synthetic data generator for the data lake pipeline."""

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np


def generate_sample_data(
    num_records: int = 1000,
    output_path: str = "data/input/sample.csv",
    seed: int = 42,
    start_date: str = "2024-01-01",
    include_invalid: bool = True,
    include_duplicates: bool = True,
) -> str:
    """
    Generate synthetic event data with configurable edge cases.

    Args:
        num_records: Number of records to generate
        output_path: Path to write the output CSV
        seed: Random seed for reproducibility
        start_date: Start date for event timestamps
        include_invalid: Whether to include deliberately invalid records
        include_duplicates: Whether to include duplicate records

    Returns:
        Path to the generated file
    """
    np.random.seed(seed)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    statuses = ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]
    entities = [f"ENT_{i:05d}" for i in range(1, 101)]

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    records: List[dict] = []

    for i in range(num_records):
        # Base event
        event_id = f"EVT_{i:08d}"
        days_offset = np.random.randint(0, 90)
        hours_offset = np.random.randint(0, 24)
        minutes_offset = np.random.randint(0, 60)
        event_ts = start_dt + timedelta(
            days=days_offset, hours=hours_offset, minutes=minutes_offset
        )
        entity_id = np.random.choice(entities)
        amount = round(np.random.uniform(1.0, 10000.0), 2)
        status = np.random.choice(statuses, p=[0.1, 0.7, 0.1, 0.1])
        source_file = "sample_data.csv"

        records.append({
            "event_id": event_id,
            "event_timestamp": event_ts.isoformat(),
            "entity_id": entity_id,
            "amount": amount,
            "status": status,
            "source_file": source_file,
        })

    # Add invalid records
    if include_invalid and num_records > 10:
        # Null event_id
        records.append({
            "event_id": None,
            "event_timestamp": (start_dt + timedelta(days=1)).isoformat(),
            "entity_id": "ENT_00001",
            "amount": 100.0,
            "status": "COMPLETED",
            "source_file": "sample_data.csv",
        })
        # Negative amount
        records.append({
            "event_id": f"EVT_INVALID_{np.random.randint(1000,9999)}",
            "event_timestamp": (start_dt + timedelta(days=2)).isoformat(),
            "entity_id": "ENT_00002",
            "amount": -50.0,
            "status": "COMPLETED",
            "source_file": "sample_data.csv",
        })
        # Invalid status
        records.append({
            "event_id": f"EVT_INVALID_{np.random.randint(1000,9999)}",
            "event_timestamp": (start_dt + timedelta(days=3)).isoformat(),
            "entity_id": "ENT_00003",
            "amount": 200.0,
            "status": "UNKNOWN",
            "source_file": "sample_data.csv",
        })
        # Malformed timestamp
        records.append({
            "event_id": f"EVT_INVALID_{np.random.randint(1000,9999)}",
            "event_timestamp": "not-a-timestamp",
            "entity_id": "ENT_00004",
            "amount": 300.0,
            "status": "PENDING",
            "source_file": "sample_data.csv",
        })
        # Missing entity_id
        records.append({
            "event_id": f"EVT_INVALID_{np.random.randint(1000,9999)}",
            "event_timestamp": (start_dt + timedelta(days=4)).isoformat(),
            "entity_id": None,
            "amount": 400.0,
            "status": "FAILED",
            "source_file": "sample_data.csv",
        })

    # Add duplicates
    if include_duplicates and num_records > 10:
        for _ in range(min(5, num_records // 20)):
            dup_record = dict(np.random.choice(records))
            dup_record["event_id"] = f"DUP_{dup_record['event_id']}"
            records.append(dup_record)

    # Shuffle records
    np.random.shuffle(records)

    # Write to CSV
    fieldnames = [
        "event_id", "event_timestamp", "entity_id", "amount", "status", "source_file"
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

    print(f"Generated {len(records)} records -> {output_path}")
    print(f"  Valid records:  {num_records}")
    print(f"  Invalid records: {len(records) - num_records}")
    print(f"  Seed: {seed}")

    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic sample data")
    parser.add_argument(
        "--records", type=int, default=1000, help="Number of valid records to generate"
    )
    parser.add_argument(
        "--output", default="data/input/sample.csv", help="Output file path"
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--start-date", default="2024-01-01", help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--no-invalid", action="store_true", help="Skip invalid records"
    )
    parser.add_argument(
        "--no-duplicates", action="store_true", help="Skip duplicate records"
    )

    args = parser.parse_args()

    generate_sample_data(
        num_records=args.records,
        output_path=args.output,
        seed=args.seed,
        start_date=args.start_date,
        include_invalid=not args.no_invalid,
        include_duplicates=not args.no_duplicates,
    )


if __name__ == "__main__":
    main()