"""
This module provides shared functions that modify NetSuite field values.
"""

### IMPORTS ###
import numpy as np
import pandas as pd


# delegating to each module to simplify __init__.py
__all__ = [
    "convert_json_strings_to_python_types",
    "set_subsidiary_by_location",
    "add_category_levels_and_vsi_info",
]


# FUNCTIONS
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


def set_subsidiary_by_location(df: pd.DataFrame, location_map: dict,
                               null_value: str = "null", replacement_value = "Not Specified") -> pd.DataFrame:
    """
    Sets the subsidiary name based on the location of the transaction.

    Args:
        df (pd.DataFrame): The DataFrame containing company data.
        location_map (dict): The dictionary that holds the mapping of locations to subsidiaries.
        null_value (str): The value to use for null locations. Defaults to "null".
        replacement_value: The value to use for locations not found in the location_map. Defaults to "Not Specified".

    Returns:
        pd.DataFrame: The DataFrame with the 'Subsidiary' column updated based on the location.
    """

    # Replace subsidiary_name only if location is not "null"
    df["subsidiary_name"] = np.where(
        df["location"] == null_value,
        df["subsidiary_name"],
        df["location"].map(location_map)
    )

    return df


def add_category_levels_and_vsi_info(line_item_df: pd.DataFrame, item_master_df: pd.DataFrame) -> pd.DataFrame:
    """
    I should separate these so that the function does only one thing, but it's the same code, so...
    
    Updates a line item dataframe by cleaning, rationalizing, and merging additional
    fields from a master item dataframe.

    The function performs two primary operations:
    1. Removes specific columns (manufacturer and category levels 1-3) from the line
       item dataframe.
    2. Merges cleaned and rationalized data, including manufacturer, category levels
       1-6, vsi_mfr, and vsi_item_type columns, from the item master dataframe back
       to the line item dataframe based on the shared 'sku' key.

    Args:
        line_item_df (pd.DataFrame): Input dataframe containing line item data.
        item_master_df (pd.DataFrame): Master dataframe containing item information
            for cleaning and enriching line item data.

    Returns:
        pd.DataFrame: Updated dataframe with cleaned and enriched fields.
    """

    # drop manufacturer and category levels 1-3
    cols = [f"level_{i}_category" for i in range(1, 4)]
    line_item_df = line_item_df.drop(columns=cols)

    # add category levels 1-6 and vsi_mfr and vsi_item_type columns from item master
    cols = ["sku", "vsi_mfr", "vsi_item_category"] + [f"level_{i}_category" for i in range(1, 7)]
    line_item_df = line_item_df.merge(item_master_df[cols], how="left", on="sku")

    return line_item_df