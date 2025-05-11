# IMPORTS
# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data cleaning libraries
from common.utils.data_cleansing import clean_dataframe

# config
import common.config
from common.utils.configuration_management import load_config


# MAIN
def main():

    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    print("Retrieving repaired item data...")
    items = adl.get_parquet_file_from_data_lake(file_system_client, "raw/netsuite", "item_repaired.parquet")

    print("Cleaning repaired item data...")
    items = clean_dataframe(items, "item")

    print("Saving cleaned item data...")
    adl.save_df_as_parquet_in_data_lake(items, file_system_client, "cleaned/netsuite", f"item_cleaned.parquet")


if __name__ == "__main__":
    main()