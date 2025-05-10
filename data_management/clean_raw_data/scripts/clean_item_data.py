# IMPORTS
# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data cleaning libraries
from common.utils.data_cleansing import clean_and_filter_dataframe

# config
import common.config
from common.utils.configuration_management import load_config


# MAIN
def main():

    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    print("Retrieving raw item data...")
    items = adl.get_parquet_file_from_data_lake(file_system_client, "raw/netsuite", "item_raw.parquet")

    print("Cleaning raw item data...")
    table_field_types = load_config(common.config, "table_field_types.json")
    items = clean_and_filter_dataframe(items, "item", table_field_types)

    print("Saving cleaned item data...")
    adl.save_df_as_parquet_in_data_lake(items, file_system_client, "cleaned/netsuite", f"item_cleaned.parquet")


if __name__ == "__main__":
    main()