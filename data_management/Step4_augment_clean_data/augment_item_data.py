# IMPORTS
# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data augmentation
from common.utils.data_augmentation import add_new_category_levels

# config
import common.config
from common.utils.configuration_management import load_config


# FUNCTIONS
# MAIN
def main():

    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    print("Getting item data...")
    items = adl.get_parquet_file_from_data_lake(file_system_client, "cleaned/netsuite", "item_cleaned.parquet")

    print("Getting new category levels...")
    level_info = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite", "new_item_categories.parquet")

    print("Augmenting item data...")
    items = add_new_category_levels(items, level_info)

    # save in the data lake
    print("Saving enhanced item data in data lake...")
    adl.save_df_as_parquet_in_data_lake(items, file_system_client, "enhanced/netsuite", "item_enhanced.parquet")


if __name__ == "__main__":
    main()