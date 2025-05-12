# Standard libraries
import datetime
import argparse

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data cleaning libraries
from common.utils.data_cleansing import round_float_columns, drop_dataframe_columns, clean_dataframe

# Data analysis libraries
from pandas import DataFrame

# config
import common.config
from common.utils.configuration_management import load_config


# FUNCTIONS
def curate_line_items(df: DataFrame) -> DataFrame:
    """
    Filters and curates a DataFrame of line items based on financial and subsidiary
    name criteria.

    Filters are applied to ensure the data contains only records that meet the
    following conditions:
    1. The total cost is greater than 0.
    2. The gross profit percentage is not negative infinity.
    3. The gross profit percentage is greater than or equal to -50.
    4. The subsidiary name is not "Not Specified".

    Args:
        df (DataFrame): The input DataFrame containing line item data.

    Returns:
        DataFrame: A curated DataFrame with only rows that satisfy the filtering
        criteria.
    """
    df = df[
        (df.total_cost > 0) &
        (df.gross_profit_percent != float('-inf')) &
        (df.gross_profit_percent >= -50) &
        (df.subsidiary_name != "Not Specified")
    ]

    return df


# MAIN
def main() -> None:
    """
    Main function to process and curate transaction data from a data lake.

    This function handles command-line argument parsing to specify the transaction 
    type to process. It connects to a data lake using the provided configuration, 
    retrieves transaction data, applies curation logic to the data, and saves the 
    curated data back to the data lake.

    Returns:
        None

    Raises:
        argparse.ArgumentError: If invalid arguments are provided when parsing
            command-line inputs.
        
    Command-line Arguments:
        --trans-type: Optional str, one of ['Estimate', 'SalesOrd', 'CustInvc']
            Transaction type to process. If not specified, all types will be processed.
    """

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process transaction data.')
    parser.add_argument('--trans-type', type=str, choices=['Estimate', 'SalesOrd', 'CustInvc'],
                        help='Transaction type to process. If not specified, all types will be processed.')
    args = parser.parse_args()

    print(f"\rAttaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    # Define transaction types
    transaction_types = ["Estimate", "SalesOrd", "CustInvc"]

    # get transaction-level and line item data
    trans_types_to_process = [args.trans_type] if args.trans_type else transaction_types

    cur_data_state = "enhanced"
    new_data_state = "curated"
    for trans_type in trans_types_to_process:
        print(f"Getting {trans_type} line items from data lake...")
        line_items = adl.get_parquet_file_from_data_lake(file_system_client, f"{cur_data_state}/netsuite",
                                                 f"transaction/{trans_type}ItemLineItems_{cur_data_state}.parquet")

        print(f"Curating {trans_type} line items...")
        line_items = curate_line_items(line_items)


        print(f"\rSaving curated {trans_type} line items in data lake...")
        adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, f"{new_data_state}/netsuite",
                                            f"transaction/{trans_type}ItemLineItems_{new_data_state}.parquet")


if __name__ == "__main__":
    main()
