"""
This module provides shared functions that modify NetSuite field values.
"""

### IMPORTS ###
import numpy as np
import pandas as pd


# FUNCTIONS
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
    cols = ["manufacturer"] + [f"level_{i}_category" for i in range(1, 4)]
    line_item_df = line_item_df.drop(columns=cols)

    # add back cleaned/rationalized manufacturer and levels 1-6 and vsi_mfr and vsi_item_type columns from item master
    cols = ["sku", "manufacturer", "vsi_mfr", "vsi_item_category"] + [f"level_{i}_category" for i in range(1, 7)]
    line_item_df = line_item_df.merge(item_master_df[cols], how="left", on="sku")

    return line_item_df