"""Operational metrics and logging for the data pipeline."""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional


class RunMetrics:
    """Tracks and persists operational metrics for a pipeline run."""

    def __init__(self, run_id: Optional[str] = None, metrics_dir: str = "data/metrics"):
        self.run_id = run_id or str(uuid.uuid4())
        self.start_time = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.metrics_dir = metrics_dir
        self.input_count = 0
        self.output_count = 0
        self.rejected_count = 0
        self.status = "RUNNING"
        self.failure_reason: Optional[str] = None
        self.errors: list[Dict[str, Any]] = []

        os.makedirs(self.metrics_dir, exist_ok=True)

    def record_input(self, count: int) -> None:
        """Record the number of input records."""
        self.input_count = count

    def record_output(self, count: int) -> None:
        """Record the number of output records."""
        self.output_count = count

    def record_rejected(self, count: int) -> None:
        """Record the number of rejected records."""
        self.rejected_count = count

    def add_error(self, error: Dict[str, Any]) -> None:
        """Add an error entry."""
        self.errors.append(error)

    def complete(self, status: str = "SUCCESS", failure_reason: Optional[str] = None) -> None:
        """Mark the run as complete."""
        self.end_time = datetime.now(timezone.utc)
        self.status = status
        self.failure_reason = failure_reason

    def to_dict(self) -> Dict[str, Any]:
        """Serialize metrics to a dictionary."""
        return {
            "run_id": self.run_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "input_count": self.input_count,
            "output_count": self.output_count,
            "rejected_count": self.rejected_count,
            "status": self.status,
            "failure_reason": self.failure_reason,
            "errors": self.errors,
            "duration_seconds": (
                (self.end_time - self.start_time).total_seconds()
                if self.end_time
                else None
            ),
        }

    def save(self) -> str:
        """Save metrics to a JSON file and return the file path."""
        os.makedirs(self.metrics_dir, exist_ok=True)
        file_path = os.path.join(self.metrics_dir, f"run_{self.run_id}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        return file_path

    @classmethod
    def load(cls, run_id: str, metrics_dir: str = "data/metrics") -> "RunMetrics":
        """Load metrics from a JSON file."""
        file_path = os.path.join(metrics_dir, f"run_{run_id}.json")
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        metrics = cls(run_id=run_id, metrics_dir=metrics_dir)
        metrics.start_time = datetime.fromisoformat(data["start_time"])
        metrics.end_time = (
            datetime.fromisoformat(data["end_time"]) if data.get("end_time") else None
        )
        metrics.input_count = data.get("input_count", 0)
        metrics.output_count = data.get("output_count", 0)
        metrics.rejected_count = data.get("rejected_count", 0)
        metrics.status = data.get("status", "RUNNING")
        metrics.failure_reason = data.get("failure_reason")
        metrics.errors = data.get("errors", [])
        return metrics