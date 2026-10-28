"""Reconciliation script - compares source input to target output."""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd


def read_partitioned_parquet(output_path: str) -> pd.DataFrame:
    """Read all parquet files from a partitioned directory."""
    all_dfs: List[pd.DataFrame] = []
    for root, _dirs, files in os.walk(output_path):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                df = pd.read_parquet(file_path)
                all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def read_quarantine(quarantine_path: str) -> pd.DataFrame:
    """Read all parquet files from quarantine directory."""
    if not os.path.exists(quarantine_path):
        return pd.DataFrame()

    all_dfs: List[pd.DataFrame] = []
    for root, _dirs, files in os.walk(quarantine_path):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                df = pd.read_parquet(file_path)
                all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def reconcile(
    input_path: str,
    output_path: str,
    quarantine_path: str = "data/quarantine",
    report_path: str = "data/reconciliation_report.json",
) -> Dict[str, Any]:
    """
    Reconcile source input against target output and quarantine.

    Returns a reconciliation report dictionary.
    """
    # Read source
    if input_path.endswith(".csv"):
        source_df = pd.read_csv(input_path)
    elif input_path.endswith(".json"):
        source_df = pd.read_json(input_path)
    else:
        raise ValueError(f"Unsupported input format: {input_path}")

    source_count = len(source_df)
    source_file = os.path.basename(input_path)

    # Read target
    target_df = read_partitioned_parquet(output_path)
    target_count = len(target_df)

    # Read quarantine
    quarantine_df = read_quarantine(quarantine_path)
    quarantine_count = len(quarantine_df)

    # Calculate metrics
    total_accounted = target_count + quarantine_count
    unreconciled = source_count - total_accounted

    # Check for duplicates in target
    target_duplicates = 0
    if "event_id" in target_df.columns and not target_df.empty:
        target_duplicates = int(target_df.duplicated(subset=["event_id"]).sum())

    # Check entity referential integrity
    orphaned_entities = 0
    if not target_df.empty and "entity_id" in target_df.columns:
        # Simple check: entity_id should not be null
        orphaned_entities = int(target_df["entity_id"].isna().sum())

    # Partition distribution
    partition_distribution: Dict[str, int] = {}
    if not target_df.empty and "event_date" in target_df.columns:
        partition_distribution = target_df["event_date"].value_counts().to_dict()

    report: Dict[str, Any] = {
        "reconciliation_id": f"recon_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        "source_file": source_file,
        "source_count": source_count,
        "target_count": target_count,
        "quarantine_count": quarantine_count,
        "total_accounted": total_accounted,
        "unreconciled": unreconciled,
        "reconciled": unreconciled == 0,
        "target_duplicates": target_duplicates,
        "orphaned_entities": orphaned_entities,
        "partition_distribution": partition_distribution,
        "reconciled_at": datetime.now(timezone.utc).isoformat(),
    }

    # Save report
    os.makedirs(os.path.dirname(report_path) if os.path.dirname(report_path) else ".", exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report


def main():
    parser = argparse.ArgumentParser(description="Reconcile pipeline input to output")
    parser.add_argument(
        "--input", required=True, help="Path to source input file"
    )
    parser.add_argument(
        "--output", default="data/output", help="Path to partitioned output directory"
    )
    parser.add_argument(
        "--quarantine", default="data/quarantine", help="Path to quarantine directory"
    )
    parser.add_argument(
        "--report", default="data/reconciliation_report.json", help="Output report path"
    )

    args = parser.parse_args()

    report = reconcile(
        input_path=args.input,
        output_path=args.output,
        quarantine_path=args.quarantine,
        report_path=args.report,
    )

    print("\n=== Reconciliation Report ===")
    print(f"Source file:      {report['source_file']}")
    print(f"Source count:     {report['source_count']}")
    print(f"Target count:     {report['target_count']}")
    print(f"Quarantine count: {report['quarantine_count']}")
    print(f"Total accounted:  {report['total_accounted']}")
    print(f"Unreconciled:     {report['unreconciled']}")
    print(f"Reconciled:       {report['reconciled']}")
    print(f"Target duplicates: {report['target_duplicates']}")
    print(f"Orphaned entities: {report['orphaned_entities']}")
    print(f"Report saved to:  {args.report}")

    sys.exit(0 if report["reconciled"] else 1)


if __name__ == "__main__":
    main()