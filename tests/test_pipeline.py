"""Tests for the data pipeline."""

import os
import tempfile

import pandas as pd
import pytest

from src.pipeline import DataLakePipeline


@pytest.fixture
def sample_csv():
    data = pd.DataFrame([
        {
            "event_id": "EVT_001",
            "event_timestamp": "2024-01-15T10:00:00Z",
            "entity_id": "ENT_001",
            "amount": 100.00,
            "status": "COMPLETED",
            "source_file": "test.csv",
        },
        {
            "event_id": "EVT_002",
            "event_timestamp": "2024-01-15T11:00:00Z",
            "entity_id": "ENT_002",
            "amount": 200.00,
            "status": "PENDING",
            "source_file": "test.csv",
        },
        {
            "event_id": "EVT_003",
            "event_timestamp": "2024-01-16T12:00:00Z",
            "entity_id": "ENT_003",
            "amount": 300.00,
            "status": "FAILED",
            "source_file": "test.csv",
        },
    ])
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    data.to_csv(tmp.name, index=False)
    tmp.close()
    yield tmp.name
    os.unlink(tmp.name)


@pytest.fixture
def invalid_csv():
    data = pd.DataFrame([
        {
            "event_id": None,
            "event_timestamp": "2024-01-15T10:00:00Z",
            "entity_id": "ENT_001",
            "amount": 100.00,
            "status": "COMPLETED",
            "source_file": "test.csv",
        },
        {
            "event_id": "EVT_002",
            "event_timestamp": "not-a-timestamp",
            "entity_id": "ENT_002",
            "amount": -50.00,
            "status": "INVALID",
            "source_file": "test.csv",
        },
    ])
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    data.to_csv(tmp.name, index=False)
    tmp.close()
    yield tmp.name
    os.unlink(tmp.name)

class TestPipeline:
    def test_happy_path(self, sample_csv):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "output")
            quarantine = os.path.join(tmpdir, "quarantine")
            metrics = os.path.join(tmpdir, "metrics")
            state = os.path.join(tmpdir, "state")

            pipeline = DataLakePipeline(
                input_path=sample_csv,
                output_path=output,
                quarantine_path=quarantine,
                metrics_dir=metrics,
                state_dir=state,
            )
            result = pipeline.run()

            assert result["status"] == "SUCCESS"
            assert result["input_count"] == 3
            assert result["output_count"] == 3
            assert result["rejected_count"] == 0

            parquet_files = []
            for root, _dirs, files in os.walk(output):
                for f in files:
                    if f.endswith(".parquet"):
                        parquet_files.append(os.path.join(root, f))
            assert len(parquet_files) > 0

    def test_invalid_records_quarantined(self, invalid_csv):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "output")
            quarantine = os.path.join(tmpdir, "quarantine")
            metrics = os.path.join(tmpdir, "metrics")
            state = os.path.join(tmpdir, "state")

            pipeline = DataLakePipeline(
                input_path=invalid_csv,
                output_path=output,
                quarantine_path=quarantine,
                metrics_dir=metrics,
                state_dir=state,
            )
            result = pipeline.run()

            assert result["status"] == "SUCCESS"
            assert result["rejected_count"] > 0

            quarantine_files = []
            for root, _dirs, files in os.walk(quarantine):
                for f in files:
                    if f.endswith(".parquet"):
                        quarantine_files.append(os.path.join(root, f))
            assert len(quarantine_files) > 0

    def test_idempotent_rerun(self, sample_csv):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "output")
            quarantine = os.path.join(tmpdir, "quarantine")
            metrics = os.path.join(tmpdir, "metrics")
            state = os.path.join(tmpdir, "state")

            pipeline = DataLakePipeline(
                input_path=sample_csv,
                output_path=output,
                quarantine_path=quarantine,
                metrics_dir=metrics,
                state_dir=state,
            )

            result1 = pipeline.run()
            assert result1["status"] == "SUCCESS"

            result2 = pipeline.run()
            assert result2["status"] == "SKIPPED"
            assert result2["output_count"] == 0

    def test_empty_input(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_csv = os.path.join(tmpdir, "empty.csv")
            pd.DataFrame(columns=[
                "event_id", "event_timestamp", "entity_id",
                "amount", "status", "source_file"
            ]).to_csv(empty_csv, index=False)

            output = os.path.join(tmpdir, "output")
            quarantine = os.path.join(tmpdir, "quarantine")
            metrics = os.path.join(tmpdir, "metrics")
            state = os.path.join(tmpdir, "state")

            pipeline = DataLakePipeline(
                input_path=empty_csv,
                output_path=output,
                quarantine_path=quarantine,
                metrics_dir=metrics,
                state_dir=state,
            )
            result = pipeline.run()

            assert result["status"] == "SUCCESS"
            assert result["input_count"] == 0
            assert result["output_count"] == 0
            assert result["rejected_count"] == 0

    def test_partitioning_creates_directories(self, sample_csv):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "output")
            quarantine = os.path.join(tmpdir, "quarantine")
            metrics = os.path.join(tmpdir, "metrics")
            state = os.path.join(tmpdir, "state")

            pipeline = DataLakePipeline(
                input_path=sample_csv,
                output_path=output,
                quarantine_path=quarantine,
                metrics_dir=metrics,
                state_dir=state,
            )
            result = pipeline.run()

            assert os.path.exists(output)
            subdirs = []
            for root, dirs, _files in os.walk(output):
                for d in dirs:
                    subdirs.append(os.path.join(root, d))
            assert len(subdirs) > 0
            year_dirs = [d for d in subdirs if d.endswith("year=2024") or "year=" in d]
            assert len(year_dirs) > 0