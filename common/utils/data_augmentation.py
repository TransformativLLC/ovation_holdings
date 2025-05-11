"""
This module provides data augmentation functions shared across the project.
"""

### IMPORTS ###
# data manipulation
from pandas import DataFrame


# delegating to each module to simplify __init__.py
__all__ = [
    "add_new_category_levels",
    "add_vsi_item_category",
]


### FUNCTIONS ###
def add_new_category_levels(df: DataFrame, level_info: DataFrame) -> DataFrame:
    """
    Adds new category levels to an input DataFrame and updates matching category data
    based on a master lookup DataFrame.

    The function expands the input DataFrame by adding new categorical "level" columns
    with default values. It ensures the columns are rearranged for logical grouping
    and updates the category levels for matching items from a reference DataFrame. Any
    unmatched items retain their existing values with specific replacements applied
    where necessary. The resulting DataFrame maintains the original structure with
    updated data.

    Args:
        df (DataFrame): The primary DataFrame containing SKU data and category levels
            that will be enriched and updated.
        level_info (DataFrame): The reference DataFrame containing level category data
            for SKUs serving as the source for enrichment.

    Returns:
        DataFrame: A new DataFrame with added category levels and updated values based
        on the reference input.
    """

    df = df.copy()

    # add new level columns to df
    for i in [4, 5, 6]:
        df[f"level_{i}_category"] = 'Not Specified'
        df[f"level_{i}_category"] = df[f"level_{i}_category"].astype('string')

    # rearrange the columns so that the levels are contiguous
    columns_before = df.columns[0:df.columns.get_loc("level_3_category") + 1].tolist()
    level_columns = [f"level_{i}_category" for i in range(4, 7)]
    remaining_columns = [col for col in df.columns if col not in columns_before + level_columns]
    df = df[columns_before + level_columns + remaining_columns]

    # Update level categories for matching df
    # set sku as index for both df and level_info
    level_columns = [f"level_{i}_category" for i in range(1, 7)]
    df = df.set_index('sku')
    master_lookup = level_info.set_index('sku')[level_columns]

    # only SKUs present in master_lookup will overwrite
    df.update(master_lookup)
    df = df.reset_index()

    # replace old category value with the updated one for those skus that didn't match
    df["level_1_category"] = df["level_1_category"].replace("Valve", "Valves")

    return df


def add_vsi_item_category(df: DataFrame, item_master: DataFrame, new_col: str = 'vsi_item_category') -> DataFrame:
    """
    Adds a new column to a DataFrame in order to classify items into categories based
    on a provided item master DataFrame.

    This function creates a new column in the input DataFrame `df`, initializes it
    with a default value, and updates it based on a lookup with another DataFrame,
    `item_master`. The new column can be named as specified by the user, or it
    will default to 'vsi_item_category'.

    Args:
        df (DataFrame): The input DataFrame. It must contain a column named 'sku'
            which is used to match rows against the `item_master` DataFrame.
        item_master (DataFrame): The reference DataFrame which holds the
            classification data. It must contain a 'sku' column and a column named
            the same as `new_col` (or 'vsi_item_category' if not specified).
        new_col (str): The name of the new column to add for item categorization.
            Defaults to 'vsi_item_category'.

    Returns:
        DataFrame: The updated DataFrame with the new column that categorizes items
        based on the provided `item_master`.
    """

    df = df.copy()

    # add new column to df
    df[new_col] = 'Not Specified'
    df[new_col] = df[new_col].astype('string')

    # Update vsi_category for matching df
    df = df.set_index('sku')
    master_lookup = item_master.set_index('sku')[new_col]

    # only SKUs present in master_lookup will overwrite
    df.update(master_lookup)
    df = df.reset_index()

    return df
