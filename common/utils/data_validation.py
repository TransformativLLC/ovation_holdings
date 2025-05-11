"""
This module contains shared data validation functions.
"""

# IMPORTS
# Standard libraries
from typing import Any, List

# Data analysis libraries
import numpy as np
import pandas as pd
from pandas.api import types as ptypes


# delegating to each module to simplify __init__.py
__all__ = [
    "validate_dataframe_data",
    "ValidationError"
]


# CUSTOM ERROR CLASSES
class ValidationError(Exception):
    pass


# FUNCTIONS
def validate_dataframe_data(df: pd.DataFrame) -> List[str]:
    """Validates data types and null values in a pandas DataFrame.

    Checks each column in the DataFrame against its expected data type and validates that:
    1. No null values exist
    2. Values match their expected data types (float, int, datetime, string)

    Args:
        df: A pandas DataFrame to validate

    Returns:
        List[str]: List of column names that failed validation
    """

    # Get mapping of column names to their data types
    mapping = df.dtypes.to_dict()
    invalid_columns = []

    # Validate each column against its expected type
    for col, exp_dtype in mapping.items():

        # Get column data
        series = df[col]

        # Check for null values
        if series.isna().any():
            print(f"Column '{col}' contains null values.")
            invalid_columns.append(col)
            continue

        def _matches(val: Any) -> bool:
            """Helper function to check if value matches expected type"""
            # Handle float type validation
            if ptypes.is_float_dtype(exp_dtype):
                return isinstance(val, (float, np.floating))
            # Handle integer type validation
            if ptypes.is_integer_dtype(exp_dtype):
                return isinstance(val, (int, np.integer))
            # Handle datetime type validation
            if ptypes.is_datetime64_any_dtype(exp_dtype):
                return isinstance(val, (pd.Timestamp, np.datetime64))
            # Handle string type validation
            if ptypes.is_string_dtype(exp_dtype):
                return isinstance(val, str)
            return True  # No strict check for other dtypes

        # Validate each value in the column matches the expected type
        for idx, val in series.items():
            if not _matches(val):
                print(
                    f"Column '{col}', index {idx}: expected {exp_dtype}, "
                    f"got {val!r} ({type(val)})"
                )
                invalid_columns.append(col)
                break

    return invalid_columns