"""
This module provides data augmentation functions shared across the project.
"""

### IMPORTS ###
# data manipulation
import pandas as pd


# delegating to each module to simplify __init__.py
__all__ = [
    "add_item_master_fields",
]


### FUNCTIONS ###
def add_item_master_fields(line_items: pd.DataFrame, item_master: pd.DataFrame) -> pd.DataFrame:
    """
    Adds or updates item master fields to the given line items DataFrame.

    This function modifies a line items DataFrame by adding certain predefined
    columns corresponding to item master details. The columns include category
    levels and other specific attributes. If corresponding SKUs exist in the
    item master DataFrame, their values will overwrite the defaults in the line
    items DataFrame. Any SKU not found in the item master will retain its default
    values for the added fields.

    Args:
        line_items (pd.DataFrame): A DataFrame containing line items, including a
            'sku' column to match against the item master data.
        item_master (pd.DataFrame): A DataFrame containing item master details,
            including a 'sku' column as well as fields to overwrite.

    Returns:
        pd.DataFrame: The updated line items DataFrame with item master fields
        added or updated, retaining all other original data.
    """
    # copy original
    df = line_items.copy()

    # specify columns to add
    cols = [f"level_{i}_category" for i in range(4, 7)] + ["vsi_mfr", "vsi_item_category"]
    for col in cols:
        df[col] = "Not Specified"

    # expand cols to include category levels 1-3 that already exist
    cols = [f"level_{i}_category" for i in range(1, 4)] + cols

    # set sku as index for both line items and item_master
    df = df.set_index('sku')
    master_lookup = item_master.set_index('sku')[cols]

    # only SKUs present in master_lookup will overwrite
    df.update(master_lookup)

    return df.reset_index()
