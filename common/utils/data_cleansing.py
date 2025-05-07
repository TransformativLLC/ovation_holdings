"""
This module provides data cleansing/filtering functions shared across the project.
"""

### IMPORTS ###
import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import re


# delegating to each module to simplify __init__.py
__all__ = [
    "smart_fillna",
    "remove_illegal_chars",
    "clean_illegal_chars_in_column",
    "round_float_columns",
    "get_cutoff_date",
    "convert_json_strings_to_python_types",
]

### FUNCTIONS ###
def smart_fillna(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Fills missing values in a DataFrame based on provided data type configurations.

    This function iterates through the columns of the provided DataFrame and fills
    missing values (NaNs) according to the data transformations specified in the
    configuration dictionary. Each column's data type is matched, and default values
    are used to replace missing data. Supported data types include string, int,
    float, datetime64[ns], and bool. If the data type for a column is not specified,
    it defaults to 'string'.

    Args:
        df (pd.DataFrame): The input DataFrame that contains missing values to be
            filled.
        config (dict): A dictionary specifying data transformations. The key
            'data_transforms' in this dictionary maps each column name to its
            respective data type (e.g., 'string', 'int', 'float', 'datetime64[ns]',
            or 'bool').

    Returns:
        pd.DataFrame: A DataFrame with NaN values replaced according to the
        specified configuration.
    """

    dtypes = config['data_transforms']
    df_cols = df.columns

    for col in df_cols:
        cur_type = dtypes.get(col, 'string')
        match cur_type:
            case 'string':
                df.fillna({col: ''}, inplace=True)
            case 'int':
                df.fillna({col: 0}, inplace=True)
            case 'float':
                df.fillna({col: 0.0}, inplace=True)
            case 'datetime64[ns]':
                df.fillna({col: pd.Timestamp('1900-01-01')}, inplace=True)
            case 'bool':
                df.fillna({col: False}, inplace=True)
            case _:
                df.fillna({col: ''}, inplace=True)

    return df


def remove_illegal_chars(value: str) -> str:
    """
    Removes illegal ASCII control characters from a given string.

    This function scans the input string and removes all ASCII control characters
    that fall within the range of 0-31 and 127. These characters are typically not
    visible and may cause issues in text processing or storage.

    Args:
        value: The input string to be sanitized.

    Returns:
        A new string with illegal ASCII control characters removed.
    """
    # Remove ASCII control characters (range 0-31 and 127)
    return re.sub(r'[\x00-\x1F\x7F]', '', value)


def clean_illegal_chars_in_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Cleans a specific column in the DataFrame by removing illegal characters.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column (str): Column name to clean.

    Returns:
        pd.DataFrame: The DataFrame with the cleaned column.
    """
    df = df.copy()
    # Convert the column to string type if not already
    df[column] = df[column].astype(str).apply(remove_illegal_chars)
    return df


def round_float_columns(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
    """
    Rounds all float columns in the DataFrame to the specified number of decimal places.

    Args:
        df (pd.DataFrame): The input DataFrame.
        decimals (int, optional): The number of decimals to round to. Defaults to 2.

    Returns:
        pd.DataFrame: A new DataFrame with the float columns rounded.
    """
    df = df.copy()  # Create a copy to avoid modifying a slice of the original DataFrame.
    float_cols = df.select_dtypes(include=["float"]).columns
    df.loc[:, float_cols] = df.loc[:, float_cols].round(decimals)
    return df


def get_cutoff_date(months: int = 12) -> pd.Timestamp:
    """
    Calculates a cutoff date based on the current date and a user-defined number
    of months in the past.

    This function computes a date by subtracting the specified number of months
    from the current date, and then converts it to a pandas Timestamp. It uses
    the relativedelta function to handle the calculation of past months.

    Args:
        months (int): The number of months to subtract from the current date. Defaults to 12.

    Returns:
        pd.Timestamp: A pandas Timestamp object representing the calculated cutoff date.
    """
    cutoff_date = datetime.date.today() - relativedelta(months=months)
    return pd.to_datetime(cutoff_date, errors="coerce")


def convert_json_strings_to_python_types(df: pd.DataFrame, date_format: str = "%m/%d/%Y") -> pd.DataFrame:
    """
    Converts string representations of JSON data in a pandas DataFrame to appropriate Python
    data types. This function processes `object` columns in the DataFrame to handle values
    representing `null`, boolean, numeric, or datetime values. Specifically, it converts:

    1. String "null" values to `None`.
    2. String representations of boolean values ("T", "F", "True", "False", etc.) to Python
       booleans.
    3. String representations of numeric values to Python numeric types (int or float).
    4. String representations of dates to Python datetime objects using the specified `date_format`.

    The function handles each column in an iterative process, preserving other data types and
    columns unaffected.

    Args:
        df (pd.DataFrame): Input DataFrame having columns with potential JSON-like string
            representations of data.
        date_format (str): Date format string to parse datetime values. Defaults to "%m/%d/%Y".

    Returns:
        pd.DataFrame: A new DataFrame with updated data types for `object` columns where applicable.
    """
    df = df.copy()

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(lambda x: None if isinstance(x, str) and x.strip().lower() == "null" else x)
            df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)

    for col in df.select_dtypes(include=['object']).columns:
        s = df[col]

        # Datetime conversion with your specified format
        try:
            converted = pd.to_datetime(s, format=date_format, errors='raise')
            df[col] = converted
            continue
        except Exception as e:
            pass

        # Numeric
        try:
            converted = pd.to_numeric(s, errors='raise')
            df[col] = converted
            continue
        except Exception:
            pass

        # Boolean
        if s.dropna().isin(['T', 'F', 'True', 'False', 'true', 'false']).all():
            df[col] = s.map({
                'T': True, 'F': False,
                'True': True, 'False': False,
                'true': True, 'false': False
            })

    return df
