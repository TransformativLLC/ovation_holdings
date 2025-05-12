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
def clean_and_filter_transactions(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """
    Filters and cleans a transactions DataFrame by removing transactions outside of the
    specified date range, dropping unnecessary columns, and rounding float values.

    Args:
        df (DataFrame): The input DataFrame containing transaction data with a
            'created_date' column for filtering.
        start_date (str): The starting date in the format 'YYYY-MM-DD' to include
            transactions from.
        end_date (str): The ending date in the format 'YYYY-MM-DD' to include
            transactions up to.

    Returns:
        DataFrame: A cleaned and filtered DataFrame with transactions within the
            specified date range, unnecessary columns removed, and float values rounded.
    """
    
    # remove all transactions outside the date range
    df = df[(df["created_date"] >= start_date) & (df["created_date"] <= end_date)]
    
    # drop columns
    df = drop_dataframe_columns(df, "transaction")
    
    # round floats
    df = round_float_columns(df)
    
    return df


def clean_and_filter_line_items(df: DataFrame) -> DataFrame:
    """
    Filters and processes a DataFrame containing line item data.

    This function performs data cleaning and filtering operations on a DataFrame
    representing line df. It removes entries with invalid cost or unit price,
    excludes specific unwanted item types, drops unnecessary columns, and rounds
    float values to a consistent number of decimal places.

    Args:
        df (DataFrame): The input DataFrame containing line item data.

    Returns:
        DataFrame: The cleaned and filtered DataFrame.
    """

    # drop line_items with both cost and unit_price <= 0
    df = df.query('quote_po_rate > 0 or unit_price > 0')
    
    # drop item types that are not related to products/services
    drop_list = ["Description", "Markup", "Other Charge", "Payment", "Discount"]
    df = df[~df["item_type"].isin(drop_list)]

    # finish cleaning
    df = clean_dataframe(df, "line_item")
    
    return df


# MAIN
def main() -> None:
    """
    Processes transaction data from a data lake by retrieving, cleaning, filtering, and saving
    transaction-level and line item information. Supports specific transaction types or processes
    all types if none are specified.

    Args:
        --trans-type (str): Transaction type to process. Possible values are 'Estimate',
                            'SalesOrd', or 'CustInvc'. If not specified, all transaction types
                            will be processed.

    Raises:
        SystemExit: Raised if invalid command-line arguments are passed.
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

    print("Getting item master from data lake...")
    items = adl.get_parquet_file_from_data_lake(file_system_client, "raw/netsuite",
                                                       "item_repaired.parquet")
    
    # set date range
    start_date = "2022-01-01"
    end_date = datetime.date.today().strftime("%Y-%m-%d")

    # Define transaction types
    transaction_types = ["Estimate", "SalesOrd", "CustInvc"]

    # get transaction-level and line item data
    trans_types_to_process = [args.trans_type] if args.trans_type else transaction_types
    for trans_type in trans_types_to_process:
        print(f"Getting {trans_type} transactions and line items from data lake...")
        transactions = adl.get_parquet_file_from_data_lake(file_system_client, "raw/netsuite",
                                                    f"transaction/{trans_type}_repaired.parquet")
        line_items = adl.get_parquet_file_from_data_lake(file_system_client, "raw/netsuite",
                                                 f"transaction/{trans_type}ItemLineItems_repaired.parquet")

        # add vsi_mfr field to line df so that the manufacturer field can be resolved and cleaned
        line_items = line_items.merge(items[['sku', 'vsi_mfr']], how='left', on='sku')
        line_items['vsi_mfr'] = line_items['vsi_mfr'].fillna('Not Specified')

        print(f"Cleaning and filtering {trans_type} transactions and line items...")
        transactions = clean_and_filter_transactions(transactions, start_date, end_date)
        line_items = clean_and_filter_line_items(line_items)

        print(f"\rSaving cleaned and filtered {trans_type} transactions and line items in data lake...")
        adl.save_df_as_parquet_in_data_lake(transactions, file_system_client, "cleaned/netsuite",
                                            f"transaction/{trans_type}_cleaned.parquet")
        adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, "cleaned/netsuite",
                                            f"transaction/{trans_type}ItemLineItems_cleaned.parquet")


if __name__ == "__main__":
    main()
