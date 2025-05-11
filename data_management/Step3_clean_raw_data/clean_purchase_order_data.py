# IMPORTS

# Standard libraries
import datetime

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data cleaning
from common.utils.data_cleansing import drop_dataframe_columns, round_float_columns

# Data analysis libraries
import pandas as pd

# config
import common.config
from common.utils.configuration_management import load_config


# FUNCTIONS
def clean_and_filter_purchase_orders(transactions: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Cleans and filters purchase order transactions within a specified date range. The function removes
    transactions outside the given date range, drops unnecessary columns for purchase orders, and rounds
    float values to two decimals to ensure data consistency and correctness.

    Args:
        transactions (pd.DataFrame): A DataFrame containing transaction data with various columns and
            transaction details.
        start_date (str): A string representing the start date in the format 'YYYY-MM-DD'. Transactions
            occurring on this date or later will be retained.
        end_date (str): A string representing the end date in the format 'YYYY-MM-DD'. Transactions
            occurring on this date or earlier will be retained.

    Returns:
        pd.DataFrame: A cleaned and filtered DataFrame containing only purchase order transactions within
        the specified date range, with unnecessary columns dropped and float values rounded to two
        decimal places.
    """

    # remove all transactions outside the date range
    transactions = transactions[(transactions["created_date"] >= start_date) & (transactions["created_date"] <= end_date)]

    # most columns in the transactions don't make sense for purchase orders, so drop them
    transactions = drop_dataframe_columns(transactions, "po")

    # round floats to two decimals
    transactions = round_float_columns(transactions)

    return transactions


def clean_po_line_items(line_items: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and processes purchase order (PO) line df to prepare them for further analysis
    or storage by removing unnecessary data, filtering relevant item types, and standardizing
    the numerical formats.

    Args:
        line_items (pd.DataFrame): A pandas DataFrame containing purchase order line df that
            require cleaning and processing. The DataFrame should include columns such as
            "po_line_item" and "item_type" where necessary processing will be applied.

    Returns:
        pd.DataFrame: A cleaned and processed DataFrame with irrelevant columns removed, unwanted
            item types filtered out, and numeric values rounded to two decimal places.
    """

    # drop unneeded columns
    line_items = drop_dataframe_columns(line_items, "po_line_item")

    # drop item types that are not related to products/services
    drop_list = ["Description", "Markup", "Other Charge", "Payment", "Discount"]
    line_items = line_items[~line_items["item_type"].isin(drop_list)]

    # round floats to two decimals
    line_items = round_float_columns(line_items)

    return line_items


# MAIN
def main():

    # attach to the data lake
    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    # get transaction-level and line item data
    print("Getting purchase order data...")
    trans_type = "PurchOrd"
    transactions = adl.get_parquet_file_from_data_lake(file_system_client, "raw/netsuite",
                                                       f"transaction/{trans_type}_repaired.parquet")
    line_items = adl.get_parquet_file_from_data_lake(file_system_client, "raw/netsuite",
                                                     f"transaction/{trans_type}ItemLineItems_repaired.parquet")

    start_date = "2021-01-01" # keep 1 year extra for lookback window
    end_date = datetime.date.today().strftime("%Y-%m-%d")

    print("Cleaning purchase order transaction data...")
    transactions = clean_and_filter_purchase_orders(transactions, start_date, end_date)

    print("Cleaning purchase order line item data...")
    line_items = clean_po_line_items(line_items)

    # save in data lake
    print("Saving cleaned purchase order data in data lake...")
    adl.save_df_as_parquet_in_data_lake(transactions, file_system_client, "cleaned/netsuite",
                                        f"transaction/{trans_type}_cleaned.parquet")
    adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, "cleaned/netsuite",
                                        f"transaction/{trans_type}ItemLineItems_cleaned.parquet")


if __name__ == "__main__":
    main()