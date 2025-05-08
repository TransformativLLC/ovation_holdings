# IMPORTS

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# Data analysis libraries
import pandas as pd

# config
from common.utils.configuration_management import load_config


# MAIN
def main():

    # read spreadsheet from Rick Chadha
    print("Reading Excel spreadsheet with new category level data...")
    new_item_categories = pd.read_excel(f'data_management/client_data/NewItemLevels.xlsx')

    # make sure everything is interpreted as a string
    print("Converting and renaming categories...")
    new_item_categories = new_item_categories.astype(str)

    # rename columns to match NetSuite naming convention
    renames = {
        'Type': 'item_type',
        'Manufacturer': 'manufacturer',
        'Level 1': 'level_1_category',
        'Level 2': 'level_2_category',
        'Level 3': 'level_3_category',
        'Level 4': 'level_4_category',
        'Level 5': 'level_5_category',
        'Level 6': 'level_6_category',
        'Name': 'item_name',
        'Description': 'description',
    }
    new_item_categories.rename(columns=renames, inplace=True)

    # attach to the data lake
    print("Attaching to the data lake...")
    config = load_config("common/config/datalake_config.json", flush_cache=True)
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    # save in the data lake
    print("Saving new categories in the data lake...")
    adl.save_df_as_parquet_in_data_lake(new_item_categories, file_system_client, "enhanced/netsuite",
                                        "new_item_categories.parquet")


if __name__ == "__main__":
    main()