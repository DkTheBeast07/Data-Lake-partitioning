"""Tests for schema validation and business rules."""

import pandas as pd
import pytest

from src.validation import BusinessRuleValidator, SchemaValidator, ValidationResult


@pytest.fixture
def sample_schema():
    return {
        "event_id": {"type": "string", "nullable": False},
        "event_timestamp": {"type": "timestamp", "nullable": False},
        "entity_id": {"type": "string", "nullable": False},
        "amount": {"type": "decimal(18,2)", "nullable": True},
        "status": {
            "type": "string",
            "nullable": False,
            "valid_values": ["PENDING", "COMPLETED", "FAILED", "CANCELLED"],
        },
        "source_file": {"type": "string", "nullable": False},
    }


@pytest.fixture
def valid_df():
    return pd.DataFrame([
        {
            "event_id": "EVT_001",
            "event_timestamp": "2024-01-01T10:00:00Z",
            "entity_id": "ENT_001",
            "amount": 100.00,
            "status": "COMPLETED",
            "source_file": "test.csv",
        },
        {
            "event_id": "EVT_002",
            "event_timestamp": "2024-01-01T11:00:00Z",
            "entity_id": "ENT_002",
            "amount": 200.00,
            "status": "PENDING",
            "source_file": "test.csv",
        },
    ])


@pytest.fixture
def invalid_df():
    return pd.DataFrame([
        {
            "event_id": None,
            "event_timestamp": "2024-01-01T10:00:00Z",
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


class TestSchemaValidator:
    def test_valid_data_passes(self, sample_schema, valid_df):
        validator = SchemaValidator(sample_schema)
        result_df, result = validator.validate(valid_df, "test.csv")
        assert result.is_valid is True
        assert len(result_df) == 2
        assert len(result.rejected_records) == 0

    def test_null_required_field_rejected(self, sample_schema, invalid_df):
        validator = SchemaValidator(sample_schema)
        result_df, result = validator.validate(invalid_df, "test.csv")
        assert result.is_valid is False
        assert len(result.rejected_records) > 0
        assert any("Null value" in r for r in result.rejection_reasons)

    def test_invalid_type_rejected(self, sample_schema, invalid_df):
        validator = SchemaValidator(sample_schema)
        result_df, result = validator.validate(invalid_df, "test.csv")
        assert result.is_valid is False
        assert any("Invalid type" in r for r in result.rejection_reasons)

    def test_invalid_value_rejected(self, sample_schema, invalid_df):
        validator = SchemaValidator(sample_schema)
        result_df, result = validator.validate(invalid_df, "test.csv")
        assert result.is_valid is False
        assert any("Invalid value" in r for r in result.rejection_reasons)

    def test_missing_columns(self, sample_schema):
        validator = SchemaValidator(sample_schema)
        df = pd.DataFrame([{"event_id": "EVT_001"}])
        result_df, result = validator.validate(df, "test.csv")
        assert result.is_valid is False
        assert any("Missing required columns" in r for r in result.rejection_reasons)

    def test_duplicate_detection(self, sample_schema):
        validator = SchemaValidator(sample_schema)
        df = pd.DataFrame([
            {"event_id": "EVT_001", "event_timestamp": "2024-01-01T10:00:00Z",
             "entity_id": "ENT_001", "amount": 100.0, "status": "COMPLETED",
             "source_file": "test.csv"},
            {"event_id": "EVT_001", "event_timestamp": "2024-01-01T10:00:00Z",
             "entity_id": "ENT_001", "amount": 100.0, "status": "COMPLETED",
             "source_file": "test.csv"},
        ])
        unique_df, dup_df = validator.check_duplicates(df, ["event_id"])
        assert len(unique_df) == 1
        assert len(dup_df) == 1


class TestBusinessRuleValidator:
    def test_positive_amount_rule(self):
        validator = BusinessRuleValidator(rules=[
            {
                "name": "positive_amount",
                "condition": "amount > 0 or amount.isna()",
                "error_message": "Amount must be positive",
            }
        ])
        df = pd.DataFrame([
            {"amount": 100.0},
            {"amount": -50.0},
            {"amount": None},
        ])
        valid_df, result = validator.validate(df)
        assert len(valid_df) == 2  # 100.0 and None pass
        assert len(result.rejected_records) == 1  # -50.0 rejected

    def test_valid_status_rule(self):
        validator = BusinessRuleValidator(rules=[
            {
                "name": "valid_status",
                "condition": "status in ['PENDING', 'COMPLETED', 'FAILED', 'CANCELLED']",
                "error_message": "Invalid status",
            }
        ])
        df = pd.DataFrame([
            {"status": "COMPLETED"},
            {"status": "UNKNOWN"},
            {"status": "PENDING"},
        ])
        valid_df, result = validator.validate(df)
        assert len(valid_df) == 2
        assert len(result.rejected_records) == 1

    def test_empty_rules(self):
        validator = BusinessRuleValidator(rules=[])
        df = pd.DataFrame({"col": [1, 2, 3]})
        valid_df, result = validator.validate(df)
        assert result.is_valid is True
        assert len(valid_df) == 3