"""Idempotent load, retry, and backfill logic for the data pipeline."""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd


class IdempotentLoader:
    """Ensures pipeline runs are idempotent - reruns don't create duplicates."""

    def __init__(self, state_dir: str = "data/state"):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    def _run_state_path(self, run_id: str) -> str:
        return os.path.join(self.state_dir, f"run_{run_id}.json")

    def _input_hash_path(self, input_hash: str) -> str:
        return os.path.join(self.state_dir, f"input_{input_hash}.json")

    def compute_input_hash(self, df: pd.DataFrame, source_file: str) -> str:
        """Compute a hash of the input data for idempotency checking."""
        # Sort by all columns to ensure deterministic hash
        sorted_df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
        content = f"{source_file}:{sorted_df.to_json(orient='records')}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    def is_input_processed(self, input_hash: str) -> bool:
        """Check if this input data has already been processed."""
        path = self._input_hash_path(input_hash)
        return os.path.exists(path)

    def mark_input_processed(
        self,
        input_hash: str,
        run_id: str,
        source_file: str,
        record_count: int,
    ) -> None:
        """Record that this input data has been processed."""
        path = self._input_hash_path(input_hash)
        state = {
            "input_hash": input_hash,
            "run_id": run_id,
            "source_file": source_file,
            "record_count": record_count,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

    def save_run_state(
        self,
        run_id: str,
        input_hash: str,
        source_file: str,
        input_count: int,
        output_count: int,
        rejected_count: int,
        status: str,
    ) -> None:
        """Save the state of a completed run."""
        path = self._run_state_path(run_id)
        state = {
            "run_id": run_id,
            "input_hash": input_hash,
            "source_file": source_file,
            "input_count": input_count,
            "output_count": output_count,
            "rejected_count": rejected_count,
            "status": status,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

    def get_run_state(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve the state of a previous run."""
        path = self._run_state_path(run_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def list_previous_runs(self) -> List[Dict[str, Any]]:
        """List all previous run states."""
        runs: List[Dict[str, Any]] = []
        for filename in os.listdir(self.state_dir):
            if filename.startswith("run_") and filename.endswith(".json"):
                with open(os.path.join(self.state_dir, filename), "r", encoding="utf-8") as f:
                    runs.append(json.load(f))
        return sorted(runs, key=lambda x: x.get("completed_at", ""))


class RetryManager:
    """Manages retry logic for failed pipeline runs."""

    def __init__(self, max_retries: int = 3, backoff_seconds: int = 5):
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def should_retry(self, attempt: int, last_error: Optional[Exception] = None) -> bool:
        """Determine if a retry should be attempted."""
        return attempt < self.max_retries

    def get_backoff(self, attempt: int) -> int:
        """Calculate backoff time for the given attempt number."""
        return self.backoff_seconds * (2 ** attempt)