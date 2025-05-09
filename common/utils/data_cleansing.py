"""
This module provides data cleansing/filtering functions shared across the project.
"""

### IMPORTS ###

# standard libraries
import re
import datetime
from dateutil.relativedelta import relativedelta

# data manipulation
import pandas as pd

# config
from common.utils.configuration_management import load_config


# delegating to each module to simplify __init__.py
__all__ = [
    "smart_fillna",
    "remove_illegal_chars",
    "clean_illegal_chars_in_column",
    "round_float_columns",
    "get_cutoff_date",
    "clean_and_resolve_manufacturers",
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


def clean_and_resolve_manufacturers(df: pd.DataFrame) -> pd.DataFrame:

    # replace "empty" values with something more human-readable
    df["manufacturer"] = df["manufacturer"].fillna("Not Specified")
    df["manufacturer"] = df["manufacturer"].replace("null", "Not Specified")
    df["custom_manufacturer"] = df["custom_manufacturer"].replace("null", "Not Specified")
    df.loc[(df['vsi_mfr'] == "null") | (df['vsi_mfr'] == "Unknown") | (df['vsi_mfr'].isna()), 'vsi_mfr'] = "Not Specified"

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
    mfg_name_map = load_config("common/config/manufacturer_name_map.json", flush_cache=True)["manufacturer_map"]
    for correct_name, misspellings in mfg_name_map.items():
        df.loc[df['manufacturer'].isin(misspellings), 'manufacturer'] = correct_name

    return df