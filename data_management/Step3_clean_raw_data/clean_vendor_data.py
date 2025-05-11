# IMPORTS
# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data cleaning libraries
from common.utils.data_cleansing import drop_dataframe_columns

# config
import common.config
from common.utils.configuration_management import load_config


# MAIN FUNCTION
def main():

    # attach to the data lake
    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])

    container_name = "consolidated"
    file_system_client = adl.get_azure_file_system_client(service_client, container_name)

    # get customer data
    print("Getting vendor data...")
    source_folder = "raw/netsuite"
    df = adl.get_parquet_file_from_data_lake(file_system_client, source_folder, "vendor_repaired.parquet")

    print("Cleaning vendor data...")
    # drop columns that are not used
    df = drop_dataframe_columns(df, "vendor")

    # save in the data lake
    print("Saving cleaned customer data in data lake...")
    adl.save_df_as_parquet_in_data_lake(df, file_system_client, "cleaned/netsuite",
                                        "vendor_cleaned.parquet")


if __name__ == "__main__":
    main()