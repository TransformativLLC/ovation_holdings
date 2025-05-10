"""
This module provides data cleansing/filtering functions shared across the project.
"""

### IMPORTS ###

# standard libraries
import re
import datetime
from dateutil.relativedelta import relativedelta

# data cleaning/validation
from common.utils.data_modifications import convert_json_strings_to_python_types
from common.utils.data_validation import validate_dataframe_data

# data manipulation
import pandas as pd

# config
import common.config
from common.utils.configuration_management import load_config


# delegating to each module to simplify __init__.py
__all__ = [
    "remove_illegal_chars",
    "clean_illegal_chars_in_column",
    "round_float_columns",
    "get_cutoff_date",
    "clean_and_resolve_manufacturers",
    "clean_and_filter_dataframe",
    "ValidationError",
]


class ValidationError(Exception):
    pass


### FUNCTIONS ###
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


def clean_and_resolve_manufacturers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and resolves data in the "manufacturer" column of a DataFrame. Adjusts the values in the manufacturer column by
    appropriately considering custom manufacturer values and predefined mappings for misspellings.

    Args:
        df (pd.DataFrame): Input DataFrame containing a "manufacturer" column and related columns ("custom_manufacturer"
            and "vsi_mfr") to resolve manufacturer data.

    Returns:
        pd.DataFrame: The DataFrame with cleaned and resolved manufacturer data in the "manufacturer" column.
    """

    # resolve multiple manufacturer columns
    # -- put custom_manufacturer value in manufacturer if "Not Specified"
    df.loc[df["manufacturer"] == "Not Specified", "manufacturer"] = df["custom_manufacturer"]

    # -- put vsi_mfr value in manufacturer if "Not Specified" (which happens if custom_manufacturer was not specified)
    df.loc[df["manufacturer"] == "Not Specified", "manufacturer"] = df["vsi_mfr"]

    # Clean up manufacturer column
    # -- remove special characters
    df['manufacturer'] = df['manufacturer'].str.replace(r'[,.\/-]', ' ', regex=True)

    # -- remove leading/trailing spaces and replace multiple spaces with a single space
    df['manufacturer'] = df['manufacturer'].str.strip()
    df['manufacturer'] = df['manufacturer'].str.replace(r'\s+', ' ', regex=True)

    # -- capitalize the first letter of each word (removes all caps)
    df['manufacturer'] = df['manufacturer'].str.title()

    # remove all misspellings
    mfg_name_map = load_config(common.config, "manufacturer_name_map.json")["manufacturer_map"]
    for correct_name, misspellings in mfg_name_map.items():
        df.loc[df['manufacturer'].isin(misspellings), 'manufacturer'] = correct_name

    return df


def clean_and_filter_dataframe(df: pd.DataFrame, table_name: str, table_fields_map: dict) -> pd.DataFrame:
    """
    Cleans and filters a DataFrame to prepare it for further processing. Perform tasks such as converting data types,
    validating columns, resolving specific field values, filtering rows based on conditions, and rounding numerical values.

    Args:
        df (pd.DataFrame): The input DataFrame containing raw data that needs cleaning and filtering.
        table_name (str): The name of the table being processed, used for validation error messages.
        table_fields_map (dict): A mapping of column names to their expected data types, used for data type conversion.

    Returns:
        pd.DataFrame: A cleaned and filtered DataFrame ready for further processing.

    Raises:
        ValidationError: If any columns in the DataFrame fail validation.
    """

    # drop 'links' column
    df.drop('links', axis=1, inplace=True)

    # convert to appropriate data types
    field_conversions = table_fields_map[table_name]
    df = convert_json_strings_to_python_types(df, field_conversions)

    # validate dataframe data
    if bad_cols := validate_dataframe_data(df):
        raise ValidationError(f"The following columns did not pass validation in {table_name} table: {bad_cols}.")

    # move any valid manufacturer into the manufacturer field from custom or vsi fields
    df = clean_and_resolve_manufacturers(df)

    # remove df with item_names that start with "Inactivated"
    df = df[~df["item_name"].str.startswith("Inactivated")]

    # remove df with item_names that contain the word "custom"
    df = df[~df["item_name"].str.contains(r'\bcustom\b', case=False, regex=True)]

    # round floats to two decimals
    df = round_float_columns(df)

    return df