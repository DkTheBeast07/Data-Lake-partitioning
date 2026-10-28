"""Schema validation and business rule checks for the data pipeline."""

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


class ValidationResult:
    """Result of a validation check."""

    def __init__(
        self,
        is_valid: bool,
        rejected_records: Optional[pd.DataFrame] = None,
        rejection_reasons: Optional[List[str]] = None,
    ):
        self.is_valid = is_valid
        self.rejected_records = (
            rejected_records if rejected_records is not None else pd.DataFrame()
        )
        self.rejection_reasons = rejection_reasons or []


class SchemaValidator:
    """Validates DataFrame against a schema definition."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema

    def validate(
        self, df: pd.DataFrame, source_file: str = "unknown"
    ) -> Tuple[pd.DataFrame, ValidationResult]:
        """Validate a DataFrame against the schema."""
        errors: List[str] = []
        rejected_indices: List[int] = []

        required_columns = [
            col for col, spec in self.schema.items()
            if not spec.get("nullable", True)
        ]
        missing_columns = [c for c in required_columns if c not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
            return df, ValidationResult(is_valid=False, rejection_reasons=errors)

        for idx, row in df.iterrows():
            row_errors: List[str] = []

            for col in required_columns:
                if col in df.columns and pd.isna(row[col]):
                    row_errors.append(f"Null value in required field '{col}'")

            for col, spec in self.schema.items():
                if col not in df.columns:
                    continue
                expected_type = spec.get("type")
                value = row[col]
                if pd.isna(value):
                    continue

                if expected_type == "string" and not isinstance(value, str):
                    row_errors.append(f"Invalid type for '{col}': expected string")
                elif expected_type in ("decimal(18,2)", "float", "double"):
                    try:
                        float(value)
                    except (ValueError, TypeError):
                        row_errors.append(f"Invalid type for '{col}': expected numeric")
                elif expected_type == "timestamp":
                    try:
                        pd.to_datetime(value)
                    except (ValueError, TypeError):
                        row_errors.append(f"Invalid type for '{col}': expected timestamp")

            for col, spec in self.schema.items():
                if col not in df.columns:
                    continue
                valid_values = spec.get("valid_values")
                if valid_values and col in df.columns:
                    value = row[col]
                    if not pd.isna(value) and str(value) not in valid_values:
                        row_errors.append(
                            f"Invalid value for '{col}': {value} not in {valid_values}"
                        )

            if row_errors:
                rejected_indices.append(idx)
                errors.extend(row_errors)

        if rejected_indices:
            rejected_df = df.loc[rejected_indices].copy()
            valid_df = df.drop(index=rejected_indices).copy()
        else:
            rejected_df = pd.DataFrame(columns=df.columns)
            valid_df = df.copy()

        result = ValidationResult(
            is_valid=len(rejected_indices) == 0,
            rejected_records=rejected_df,
            rejection_reasons=errors,
        )
        return valid_df, result

    def check_duplicates(
        self, df: pd.DataFrame, key_columns: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Check for duplicate records based on key columns."""
        if not key_columns:
            return df, pd.DataFrame(columns=df.columns)

        duplicated_mask = df.duplicated(subset=key_columns, keep="first")
        unique_df = df[~duplicated_mask].copy()
        duplicate_df = df[duplicated_mask].copy()
        return unique_df, duplicate_df


class BusinessRuleValidator:
    """Validates domain-specific business rules."""

    def __init__(self, rules: Optional[List[Dict[str, Any]]] = None):
        self.rules = rules or []

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, ValidationResult]:
        """Apply business rules to a DataFrame."""
        errors: List[str] = []
        rejected_indices: List[int] = []

        for rule in self.rules:
            rule_name = rule.get("name", "unnamed_rule")
            condition = rule.get("condition")
            error_message = rule.get(
                "error_message", f"Business rule violated: {rule_name}"
            )

            if condition:
                try:
                    mask = (
                        df.eval(condition) if isinstance(condition, str) else condition(df)
                    )
                    invalid_mask = ~mask.fillna(False)
                    invalid_indices = df[invalid_mask].index.tolist()

                    for idx in invalid_indices:
                        if idx not in rejected_indices:
                            rejected_indices.append(idx)
                        errors.append(f"{rule_name}: {error_message} at index {idx}")

                except Exception as e:
                    errors.append(f"Rule '{rule_name}' evaluation error: {str(e)}")

        if rejected_indices:
            rejected_df = df.loc[rejected_indices].copy()
            valid_df = df.drop(index=rejected_indices).copy()
        else:
            rejected_df = pd.DataFrame(columns=df.columns)
            valid_df = df.copy()

        result = ValidationResult(
            is_valid=len(rejected_indices) == 0,
            rejected_records=rejected_df,
            rejection_reasons=errors,
        )
        return valid_df, result