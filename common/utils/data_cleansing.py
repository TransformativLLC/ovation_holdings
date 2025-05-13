"""
This module provides data cleansing/filtering functions shared across the project.
"""

### IMPORTS ###
# standard libraries
import re
import datetime
from dateutil.relativedelta import relativedelta

# data manipulation
from numpy import where
from pandas import DataFrame, Timestamp, to_datetime

# config
import common.config
from common.utils.configuration_management import load_config


# delegating to each module to simplify __init__.py
__all__ = [
    "remove_illegal_chars",
    "clean_illegal_chars_in_column",
    "drop_dataframe_columns",
    "round_float_columns",
    "get_cutoff_date",
    "clean_and_resolve_manufacturers",
    "filter_by_date_range",
    "clean_dataframe",
    "set_subsidiary_by_location",
]


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


def clean_illegal_chars_in_column(df: DataFrame, column: str) -> DataFrame:
    """
    Cleans a specific column in the DataFrame by removing illegal characters.

    Args:
        df (DataFrame): The input DataFrame.
        column (str): Column name to clean.

    Returns:
        DataFrame: The DataFrame with the cleaned column.
    """
    df = df.copy()
    # Convert the column to string type if not already
    df[column] = df[column].astype('string').apply(remove_illegal_chars)
    return df


def drop_dataframe_columns(df: DataFrame, table_name: str) -> DataFrame:
    """
    Drops specified columns from the given DataFrame based on the configuration for the given table name.

    This function reads a configuration file (table_field_drops_on_clean.json) to determine
    the columns that should be dropped from the provided DataFrame for the specified table.
    It modifies the original DataFrame in place by removing the columns listed for the given
    table name.

    Args:
        df (DataFrame): The DataFrame from which columns will be dropped.
        table_name (str): The name of the table used to find the columns to drop
            from the configuration.

    Returns:
        DataFrame: The modified DataFrame with specified columns removed.
    """
    df = df.copy()
    drop_cols = load_config(common.config, "table_field_drops_on_clean.json")[table_name]
    df.drop(drop_cols, axis=1, inplace=True)

    return df
    
    
def round_float_columns(df: DataFrame, decimals: int = 2) -> DataFrame:
    """
    Rounds all float columns in the DataFrame to the specified number of decimal places.

    Args:
        df (DataFrame): The input DataFrame.
        decimals (int, optional): The number of decimals to round to. Defaults to 2.

    Returns:
        DataFrame: A new DataFrame with the float columns rounded.
    """
    df = df.copy()  # Create a copy to avoid modifying a slice of the original DataFrame.
    float_cols = df.select_dtypes(include=["float"]).columns
    df.loc[:, float_cols] = df.loc[:, float_cols].round(decimals)
    return df


def get_cutoff_date(months: int = 12) -> Timestamp:
    """
    Calculates a cutoff date based on the current date and a user-defined number
    of months in the past.

    This function computes a date by subtracting the specified number of months
    from the current date, and then converts it to a pandas Timestamp. It uses
    the relativedelta function to handle the calculation of past months.

    Args:
        months (int): The number of months to subtract from the current date. Defaults to 12.

    Returns:
        Timestamp: A pandas Timestamp object representing the calculated cutoff date.
    """
    cutoff_date = datetime.date.today() - relativedelta(months=months)
    return to_datetime(cutoff_date, errors="coerce")


def clean_and_resolve_manufacturers(df: DataFrame) -> DataFrame:
    """
    Cleans and resolves data in the "manufacturer" column of a DataFrame. Adjusts the values in the manufacturer column by
    appropriately considering custom manufacturer values and predefined mappings for misspellings.

    Args:
        df (DataFrame): Input DataFrame containing a "manufacturer" column and related columns ("custom_manufacturer"
            and "vsi_mfr") to resolve manufacturer data.

    Returns:
        DataFrame: The DataFrame with cleaned and resolved manufacturer data in the "manufacturer" column.
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


def filter_by_date_range(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """
    Filters a DataFrame to include only rows within the specified date range.

    This function takes a DataFrame and filters rows based on the values in the
    'created_date' column. Only rows where 'created_date' falls between the
    specified start_date and end_date (inclusive) are retained. The filtered
    DataFrame is returned as output.

    Args:
        df (DataFrame): Input DataFrame containing data to be filtered.
        start_date (str): The starting date of the range in 'YYYY-MM-DD' format.
                          Rows with 'created_date' greater than or equal to this
                          date are included.
        end_date (str): The ending date of the range in 'YYYY-MM-DD' format. Rows
                        with 'created_date' less than or equal to this date are
                        included.

    Returns:
        DataFrame: A new DataFrame containing only rows where 'created_date' falls
        within the specified date range.
    """

    # remove all rows outside the date range
    df = df[(df["created_date"] >= start_date) & (df["created_date"] <= end_date)]

    return df


def clean_dataframe(df: DataFrame, table_name: str) -> DataFrame:
    """
    Cleans and processes a DataFrame for further usage by applying specific transformations
    and removing unnecessary data according to the rules defined for the input table.

    Args:
        df (DataFrame): The input DataFrame that requires cleaning and processing.
        table_name (str): The name of the table, which determines additional rules
            to be applied during the cleaning process.

    Returns:
        DataFrame: The cleaned and processed DataFrame with transformations applied as per
        the defined rules.
    """

    # move any valid manufacturer into the manufacturer field from custom or vsi fields
    df = clean_and_resolve_manufacturers(df)

    # remove df with item_names that start with "Inactivated"
    df = df[~df["item_name"].str.startswith("Inactivated")]

    # remove df with item_names that contain the word "custom"
    df = df[~df["item_name"].str.contains(r'\bcustom\b', case=False, regex=True)]

    # round floats to two decimals
    df = round_float_columns(df)

    # drop columns that are not used
    df = drop_dataframe_columns(df, table_name)
    
    # change sign on quantity for future calculations (it's -1 because it is moving out of inventory)
    if table_name == "line_item":
        df["quantity"] = df["quantity"] * -1
        
    return df


def set_subsidiary_by_location(df: DataFrame, location_map: dict, null_value: str = "Not Specified") -> DataFrame:
    """
    Maps the `location` column of a DataFrame to corresponding subsidiary names using
    a provided location-to-subsidiary mapping. If location is equal to the specified
    `null_value`, it retains the original `subsidiary_name` value.

    This function is used to standardize subsidiary names based on location, unless
    the location is flagged as unspecified with the `null_value` placeholder.

    Args:
        df (DataFrame): A pandas DataFrame containing the columns `location` and
            `subsidiary_name`.
        location_map (dict): A dictionary that maps location values to their
            corresponding subsidiary names.
        null_value (str, optional): A placeholder value used to represent
            unspecified or null locations. Defaults to "Not Specified".

    Returns:
        DataFrame: The modified DataFrame with updated `subsidiary_name` values
            based on the location-to-subsidiary mapping.
    """

    # Replace subsidiary_name only if location is not "Not Specified"
    df["subsidiary_name"] = where(
        df["location"] == null_value,
        df["subsidiary_name"],
        df["location"].map(location_map)
    )

    return df
