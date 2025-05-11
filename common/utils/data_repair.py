"""
This module provides shared functions that repair NetSuite field types and values.
"""

### IMPORTS ###
# standard libraries
from typing import Union

# data manipulation and validation
import pandas as pd
pd.set_option("future.no_silent_downcasting", True)
from pandas._libs.tslibs.np_datetime import OutOfBoundsDatetime
import common.utils.data_validation as dv


# delegating to each module to simplify __init__.py
__all__ = [
    "safe_date_parse",
    "convert_json_strings_to_python_types",
    "repair_dataframe_data",
]


# FUNCTIONS
def safe_date_parse(date_str: str) -> Union[pd.Timestamp, pd.NaT]:
    """
    Parses a date string into a pandas Timestamp object. If the date string is out of bounds
    or cannot be converted into a valid Timestamp, it returns pandas.NaT.

    Args:
        date_str (str): The date string to parse.

    Returns:
        Union[pd.Timestamp, pd.NaT]: The parsed Timestamp object or pandas.NaT if parsing fails.
    """
    try:
        ts = pd.to_datetime(date_str)
    except OutOfBoundsDatetime:
        ts = pd.NaT

    return ts


def convert_json_strings_to_python_types(df: pd.DataFrame, field_map: dict) -> pd.DataFrame:
    """
    Converts JSON string fields in a DataFrame to specified Python types. This function processes
    columns in a pandas DataFrame by replacing JSON "null" string values with a designated substitute
    value, filling any remaining nulls, and converting the field type to a specified Python type.

    Args:
        df (pd.DataFrame): The DataFrame containing JSON string fields to be processed.
        field_map (dict): A dictionary where keys are type names and values are dictionaries containing
            the 'fields' (list of column names to process) and 'null_substitute' (value to replace
            "null" strings and null entries).

    Returns:
        pd.DataFrame: A new DataFrame where the specified JSON string fields are converted to the
        desired Python types.
    """
    # copy original
    df = df.copy()

    # convert all fields
    for type_name, type_info in field_map.items():
        # Get a list of fields and null substitute value for the current type
        fields = type_info['fields']
        null_substitute = type_info['null_substitute']

        # there are some bad dates in some tables (e.g., 1/1/3032) that raise a pandas error, so need to deal with them
        if type_name == "datetime64[ns]":
            df[fields] = df[fields].apply(safe_date_parse)

        # also need to handle booleans because pandas considers any non-empty string as True, so 'T' and 'F' both
        # evaluate to True
        if type_name == "bool":
            bool_map = {"T": True, "F": False, "True": True, "False": False}

            # apply the map; entries not in the dict become NaN
            df[fields] = df[fields].replace(bool_map)

        # Replace string "null" with a substitute value and convert type
        df[fields] = df[fields].replace('null', null_substitute)
        df[fields] = df[fields].fillna(null_substitute)
        df[fields] = df[fields].astype(type_name)

    return df


def repair_dataframe_data(df: pd.DataFrame, table_name: str, table_fields_map: dict) -> pd.DataFrame:
    """
    Repairs and prepares a pandas DataFrame for further processing by dropping unnecessary columns, converting data
    to appropriate types based on a mapping, and validating the data integrity.

    Args:
        df (pd.DataFrame): The pandas DataFrame to be repaired and validated.
        table_name (str): The name of the table corresponding to the DataFrame.
        table_fields_map (dict): A mapping that defines data type conversions for each table.

    Returns:
        pd.DataFrame: The repaired and validated pandas DataFrame.

    Raises:
        dv.ValidationError: If the DataFrame contains columns that fail validation.
    """
    # drop the 'links' column inserted by NetSuite via a web query
    df.drop('links', axis=1, inplace=True)

    # convert to appropriate data types
    field_conversions = table_fields_map[table_name]
    df = convert_json_strings_to_python_types(df, field_conversions)

    # validate dataframe data
    if bad_cols := dv.validate_dataframe_data(df):
        raise dv.ValidationError(f"The following columns did not pass validation in {table_name} table: {bad_cols}.")

    return df
