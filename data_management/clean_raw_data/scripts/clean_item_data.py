# IMPORTS

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# Data analysis libraries
import pandas as pd

# Data management libraries
from common.utils.data_cleansing import round_float_columns, clean_and_resolve_manufacturers
from common.utils.data_modifications import convert_json_strings_to_python_types

# config
from common.utils.configuration_management import load_config


# FUNCTIONS
def clean_and_filter_item_data(items: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and filters item data from a pandas DataFrame.

    Args:
        items (pd.DataFrame): DataFrame containing item information with columns for
            links, costs, manufacturers, descriptions, dates, quantities etc.

    Returns:
        pd.DataFrame: Cleaned DataFrame with the following modifications:
            - Removed 'links' column
            - Resolved manufacturer/custom_manufacturer columns
            - Replaced 'null' values with 'Not Specified' in text columns
            - Converted date columns to datetime
            - Converted numeric columns to numbers
    """

    items = convert_json_strings_to_python_types(items)

    # drop 'links' column
    items.drop('links', axis=1, inplace=True)

    # fill in values for columns of interest that contain 'null'
    nulls = [
        'description', 'display_name', 'level_1_category', 'level_2_category', 'level_3_category',
        'parent_item', 'preferred_vendor', 'valve_spec_size'
    ]
    for col in nulls:
        items[col] = items[col].apply(lambda x: str(x) if not pd.isna(x) else 'Not Specified')
        items.loc[items[col] == 'null', col] = 'Not Specified'

    # fill in "empty" vsi_item_category values
    items.loc[(items['vsi_item_category'] == "null") | (items['vsi_item_category'] == "Unknown") | (items['vsi_item_category'].isna()), 'vsi_item_category'] = "Not Specified"

    # move any valid manufacturer into the manufacturer field from custom or vsi fields
    items = clean_and_resolve_manufacturers(items)

    # remove df with item_names that start with "Inactivated"
    items = items[~items["item_name"].str.startswith("Inactivated")]

    # remove df with item names that contain the word "custom"
    items = items[~items["item_name"].str.contains(r'\bcustom\b', case=False, regex=True)]

    # round floats to two decimals
    items = round_float_columns(items)

    return items


def add_new_item_levels(items: pd.DataFrame, new_item_info_path: str, create_new_columns: bool = True) -> pd.DataFrame:
    """Adds new item level categories from an Excel file to the df DataFrame.

    Args:
        items (pd.DataFrame): DataFrame containing item information with existing level categories.
        new_item_info_path (str): Path to Excel file containing new level information.
        create_new_columns (bool, optional): Whether to create new level columns 4-6. Defaults to True.

    Returns:
        pd.DataFrame: Items DataFrame with updated level categories.
    """

    if create_new_columns:
        # add new level columns to df
        for i in [4, 5, 6]:
            items[f"level_{i}_category"] = 'Not Specified'

        # rearrange the columns so that the levels are contiguous
        columns_before = items.columns[0:items.columns.get_loc("level_3_category") + 1].tolist()
        level_columns = [f"level_{i}_category" for i in [4, 5, 6]]
        remaining_columns = [col for col in items.columns if col not in columns_before + level_columns]
        items = items[columns_before + level_columns + remaining_columns]

    # get new level information in Excel
    level_info = pd.read_excel(new_item_info_path)

    # convert all level values to string (Excel treats some as numbers)
    for i in range(1, 7):
        level_info[f'Level {i}'] = level_info[f'Level {i}'].astype(str)

    # Update level categories for matching df
    for i in range(1, 7):
        items.loc[items['item_name'].isin(level_info['Name']), f'level_{i}_category'] = \
            items[items['item_name'].isin(level_info['Name'])]['item_name'].map(
                dict(zip(level_info['Name'], level_info[f'Level {i}']))
            )

    # replace nan strings with "Not Specified"
    for i in range(1, 7):
        items[f'level_{i}_category'] = items[f'level_{i}_category'].replace("nan", "Not Specified")

    # replace old category with the updated one
    items["level_1_category"] = items["level_1_category"].replace("Valve", "Valves")

    return items

# MAIN
def main():

    # attach to the data lake
    print("Attaching to data lake...")
    config = load_config("common/config/datalake_config.json", flush_cache=True)
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    # get vendor data
    print("Getting item data...")
    source_folder = "raw/netsuite"
    items = adl.get_parquet_file_from_data_lake(file_system_client, source_folder, "item_raw.parquet")


    print("Cleaning item data...")
    items = clean_and_filter_item_data(items)

    # save in the data lake
    print("Saving cleaned item data in data lake...")
    adl.save_df_as_parquet_in_data_lake(items, file_system_client, "cleaned/netsuite", "item_cleaned.parquet")

    # Technically, this should be in another script, but seems a waste of effort to do so
    # add new item category levels and save as enhanced
    print("Enhancing item data...")
    items = add_new_item_levels(items,"data_management/client_data/NewItemLevels.xlsx")

    print("Saving enhanced item data in data lake...")
    adl.save_df_as_parquet_in_data_lake(items, file_system_client, "enhanced/netsuite", "item_enhanced.parquet")


if __name__ == "__main__":
    main()