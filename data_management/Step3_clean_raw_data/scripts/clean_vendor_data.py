# IMPORTS

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# Data analysis libraries
import pandas as pd

# Data management libraries
from common.utils.data_modifications import convert_json_strings_to_python_types

# config
from common.utils.configuration_management import load_config


# FUNCTIONS
def clean_and_filter_vendor_data(vendors: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and filters vendor data from a pandas DataFrame.

    Args:
        vendors (pd.DataFrame): DataFrame containing vendor information with columns:
            links, balance, category, company_name, datecreated, etc.

    Returns:
        pd.DataFrame: Cleaned DataFrame with the following modifications:
            - Removed 'links' column
            - 'null' categories replaced with 'Not Assigned'
            - 'datecreated' converted to datetime
            - Numeric columns ('balance', 'unbilled_orders') converted to numbers
    """

    # drop 'links' column
    vendors.drop('links', axis=1, inplace=True)

    # fill in values for category that are 'null'
    vendors.loc[vendors['category'] == 'null', 'category'] = 'Not Assigned'

    vendors = convert_json_strings_to_python_types(vendors)

    return vendors

# MAIN
def main():

    # attach to the data lake
    print("Attaching to data lake...")
    config = load_config("common/config/datalake_config.json", flush_cache=True)
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    # get vendor data
    print("Getting vendor data...")
    source_folder = "raw/netsuite"
    vendors = adl.get_parquet_file_from_data_lake(file_system_client, source_folder, "vendor_raw.parquet")


    print("Cleaning vendor data...")
    vendors = clean_and_filter_vendor_data(vendors)

    # save in the data lake
    print("Saving cleaned vendor data in data lake...")
    adl.save_df_as_parquet_in_data_lake(vendors, file_system_client, "cleaned/netsuite", "vendor_cleaned.parquet")


if __name__ == "__main__":
    main()