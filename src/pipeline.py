"""Core ingestion pipeline for the time-partitioned data lake."""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd

from .metrics import RunMetrics
from .validation import BusinessRuleValidator, SchemaValidator
from .backfill import IdempotentLoader


class DataLakePipeline:
    """Main pipeline for ingesting data into a time-partitioned data lake."""

    def __init__(
        self,
        input_path: str,
        output_path: str,
        quarantine_path: str = "data/quarantine",
        metrics_dir: str = "data/metrics",
        state_dir: str = "data/state",
        partition_granularity: str = "daily",
        target_file_size_mb: int = 128,
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.quarantine_path = quarantine_path
        self.partition_granularity = partition_granularity
        self.target_file_size_mb = target_file_size_mb

        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(self.quarantine_path, exist_ok=True)

        self.metrics = RunMetrics(metrics_dir=metrics_dir)
        self.loader = IdempotentLoader(state_dir=state_dir)

        # Load schema from data dictionary
        self.schema = self._load_schema()

        # Setup validators
        self.schema_validator = SchemaValidator(self.schema)
        self.business_validator = BusinessRuleValidator(
            rules=[
                {
                    "name": "positive_amount",
                    "condition": "amount > 0 or amount.isna()",
                    "error_message": "Amount must be positive when present",
                },
                {
                    "name": "valid_status",
                    "condition": "status in ['PENDING', 'COMPLETED', 'FAILED', 'CANCELLED']",
                    "error_message": "Status must be one of: PENDING, COMPLETED, FAILED, CANCELLED",
                },
            ]
        )

    def _load_schema(self) -> Dict[str, Any]:
        """Load schema from data_dictionary.json."""
        dict_path = os.path.join(os.path.dirname(__file__), "..", "data_dictionary.json")
        dict_path = os.path.abspath(dict_path)
        with open(dict_path, "r", encoding="utf-8") as f:
            data_dict = json.load(f)
        return data_dict.get("source_schema", {})

    def read_input(self) -> pd.DataFrame:
        """Read input data from CSV or JSON file."""
        if self.input_path.endswith(".csv"):
            df = pd.read_csv(self.input_path)
        elif self.input_path.endswith(".json"):
            df = pd.read_json(self.input_path)
        else:
            raise ValueError(f"Unsupported input format: {self.input_path}")
        return df

    def transform(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Transform source data to target schema."""
        df = df.copy()
        df["run_id"] = run_id
        df["ingested_at"] = datetime.now(timezone.utc).isoformat()

        # Extract event_date from event_timestamp for partitioning
        if "event_timestamp" in df.columns:
            # Use errors='coerce' to convert invalid timestamps to NaT
            df["event_timestamp_parsed"] = pd.to_datetime(
                df["event_timestamp"], errors="coerce"
            )
            df["event_date"] = df["event_timestamp_parsed"].dt.strftime("%Y-%m-%d")
            df = df.drop(columns=["event_timestamp_parsed"])

        # Select only target columns
        target_columns = [
            "event_id", "event_date", "event_timestamp", "entity_id",
            "amount", "status", "source_file", "run_id", "ingested_at",
        ]
        available_columns = [c for c in target_columns if c in df.columns]
        df = df[available_columns]

        return df

    def write_partitioned(
        self, df: pd.DataFrame, run_id: str
    ) -> Dict[str, int]:
        """Write data to partitioned Parquet files."""
        counts_by_partition: Dict[str, int] = {}

        if df.empty:
            return counts_by_partition

        # Group by partition
        for partition_value, group in df.groupby("event_date"):
            year, month, day = partition_value.split("-")
            partition_dir = os.path.join(
                self.output_path,
                f"year={year}",
                f"month={month}",
                f"day={day}",
            )
            os.makedirs(partition_dir, exist_ok=True)

            output_file = os.path.join(partition_dir, f"part-{run_id[:8]}.parquet")
            group.to_parquet(output_file, index=False, compression="snappy")
            counts_by_partition[partition_value] = len(group)

        return counts_by_partition

    def write_quarantine(
        self, df: pd.DataFrame, run_id: str, source_file: str
    ) -> None:
        """Write rejected records to quarantine."""
        if df.empty:
            return

        os.makedirs(self.quarantine_path, exist_ok=True)
        quarantine_file = os.path.join(
            self.quarantine_path, f"rejected_{run_id[:8]}.parquet"
        )

        df["run_id"] = run_id
        df["rejected_at"] = datetime.now(timezone.utc).isoformat()
        df["source_file"] = source_file

        df.to_parquet(quarantine_file, index=False, compression="snappy")

    def run(self) -> Dict[str, Any]:
        """Execute the full pipeline."""
        source_file = os.path.basename(self.input_path)
        run_id = self.metrics.run_id

        print(f"Starting pipeline run: {run_id}")
        print(f"Input: {self.input_path}")
        print(f"Output: {self.output_path}")

        # Read input
        df = self.read_input()
        self.metrics.record_input(len(df))
        print(f"Read {len(df)} records from {source_file}")

        # Idempotency check
        input_hash = self.loader.compute_input_hash(df, source_file)
        if self.loader.is_input_processed(input_hash):
            print(f"Input already processed (hash: {input_hash}). Skipping.")
            self.metrics.status = "SKIPPED"
            self.metrics.complete()
            self.metrics.save()
            return {
                "run_id": run_id,
                "status": "SKIPPED",
                "reason": "Input already processed",
                "input_count": len(df),
                "output_count": 0,
                "rejected_count": 0,
            }

        # Schema validation
        valid_df, schema_result = self.schema_validator.validate(df, source_file)
        if schema_result.rejection_reasons:
            print(f"Schema validation: {len(schema_result.rejected_records)} records rejected")
            for reason in schema_result.rejection_reasons[:10]:
                print(f"  - {reason}")

        # Business rule validation
        valid_df, business_result = self.business_validator.validate(valid_df)
        if business_result.rejection_reasons:
            print(f"Business rule validation: {len(business_result.rejected_records)} records rejected")
            for reason in business_result.rejection_reasons[:10]:
                print(f"  - {reason}")

        # Duplicate check
        valid_df, duplicate_df = self.schema_validator.check_duplicates(
            valid_df, key_columns=["event_id"]
        )
        if not duplicate_df.empty:
            print(f"Duplicate handling: {len(duplicate_df)} duplicate records removed")

        # Transform to target schema (adds run_id, ingested_at, event_date)
        valid_df = self.transform(valid_df, run_id)

        # Catch records with invalid timestamps (NaT event_date)
        if "event_date" in valid_df.columns:
            nat_mask = valid_df["event_date"].isna()
            nat_records = valid_df[nat_mask].copy()
            valid_df = valid_df[~nat_mask].copy()
            if not nat_records.empty:
                nat_records["rejection_reason"] = "Invalid event_timestamp"
                all_rejected_list = [
                    schema_result.rejected_records,
                    business_result.rejected_records,
                    duplicate_df,
                    nat_records,
                ]
            else:
                all_rejected_list = [
                    schema_result.rejected_records,
                    business_result.rejected_records,
                    duplicate_df,
                ]
        else:
            all_rejected_list = [
                schema_result.rejected_records,
                business_result.rejected_records,
                duplicate_df,
            ]

        # Combine all rejected records (skip empty DataFrames)
        non_empty_rejected = [r for r in all_rejected_list if not r.empty]
        if non_empty_rejected:
            all_rejected = pd.concat(non_empty_rejected, ignore_index=True)
        else:
            all_rejected = pd.DataFrame(columns=valid_df.columns)
        if not all_rejected.empty:
            all_rejected = all_rejected.drop_duplicates(subset=["event_id"], keep="first")

        self.metrics.record_output(len(valid_df))
        self.metrics.record_rejected(len(all_rejected))

        # Write output
        partition_counts = self.write_partitioned(valid_df, run_id)
        self.write_quarantine(all_rejected, run_id, source_file)

        # Record idempotency state
        self.loader.mark_input_processed(
            input_hash=input_hash,
            run_id=run_id,
            source_file=source_file,
            record_count=len(df),
        )
        self.loader.save_run_state(
            run_id=run_id,
            input_hash=input_hash,
            source_file=source_file,
            input_count=len(df),
            output_count=len(valid_df),
            rejected_count=len(all_rejected),
            status="SUCCESS",
        )

        # Complete metrics
        self.metrics.complete(status="SUCCESS")
        metrics_file = self.metrics.save()

        print(f"\nPipeline completed successfully!")
        print(f"  Input records:  {len(df)}")
        print(f"  Output records: {len(valid_df)}")
        print(f"  Rejected records: {len(all_rejected)}")
        print(f"  Partitions written: {len(partition_counts)}")
        print(f"  Metrics saved to: {metrics_file}")

        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "input_count": len(df),
            "output_count": len(valid_df),
            "rejected_count": len(all_rejected),
            "partition_counts": partition_counts,
            "metrics_file": metrics_file,
        }


def main():
    """CLI entry point for the pipeline."""
    parser = argparse.ArgumentParser(
        description="Data Lake Partitioning Pipeline"
    )
    parser.add_argument(
        "--input", required=True, help="Path to input CSV or JSON file"
    )
    parser.add_argument(
        "--output", default="data/output", help="Output directory for partitioned data"
    )
    parser.add_argument(
        "--quarantine", default="data/quarantine", help="Quarantine directory for rejected records"
    )
    parser.add_argument(
        "--partition", default="daily", choices=["daily", "hourly", "monthly"],
        help="Partition granularity"
    )
    parser.add_argument(
        "--target-size", type=int, default=128,
        help="Target file size in MB"
    )

    args = parser.parse_args()

    pipeline = DataLakePipeline(
        input_path=args.input,
        output_path=args.output,
        quarantine_path=args.quarantine,
        partition_granularity=args.partition,
        target_file_size_mb=args.target_size,
    )

    try:
        result = pipeline.run()
        sys.exit(0 if result["status"] in ("SUCCESS", "SKIPPED") else 1)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()