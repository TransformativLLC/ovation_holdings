# Standard libraries
import datetime
import argparse
from typing import Literal

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data cleaning libraries
from common.utils.data_cleansing import (round_float_columns, clean_and_resolve_manufacturers,
                                         cast_object_columns_to_string, smart_fillna)
import common.utils.data_modifications as dm

# Data analysis libraries
import pandas as pd

# config
from common.utils.configuration_management import load_config


# FUNCTIONS
# could split these functions to make this file smaller, but figured it's easier to follow if it's all in one place
# REPAIR DATA
def repair_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Repairs rows in a DataFrame by converting JSON-like strings to Python types and replacing specific null values in
    the 'location' column with a default value.

    Args:
        df (pd.DataFrame): A pandas DataFrame containing the data to be processed. This DataFrame should include a
            'location' column where "null" will be replaced with "Not Specified".

    Returns:
        pd.DataFrame: A pandas DataFrame with repaired rows, where 'location' column values of "null" are replaced
        with "Not Specified," and other transformations are applied.
    """
    df = dm.convert_json_strings_to_python_types(df)

    # Cast objects to string and then smart fill all na values
    df = cast_object_columns_to_string(df)
    df = smart_fillna(df)

    # round floats to two decimals
    df = round_float_columns(df)

    return df


def repair_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Repairs transactions data by processing and cleaning a DataFrame. This function internally calls
    another function to repair rows and replaces specific null values in the `ai_order_type` column
    with a specified default value.

    Args:
        df (pd.DataFrame): The input DataFrame containing transaction data to be repaired.

    Returns:
        pd.DataFrame: The corrected and cleaned DataFrame with repaired rows and updated values in
        the `ai_order_type` column.
    """
    df = repair_rows(df)

    return df


def repair_line_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Repairs line df in the given DataFrame by adjusting specific column values.
    The function processes the DataFrame by calling a helper function to repair rows and then
    modifies the 'quantity' and 'amount' columns by converting negative values to positive.
    This ensures consistency in data representation.

    Args:
        df (pd.DataFrame): The input DataFrame containing line df with columns
            including 'quantity' and 'amount'.

    Returns:
        pd.DataFrame: The processed DataFrame with repaired rows and adjusted
        'quantity' and 'amount' values.
    """
    df = repair_rows(df)

    # change values from negative to positive
    df["quantity"] = df["quantity"] * -1
    df["amount"] = df["amount"] * -1

    return df


# CLEAN AND FILTER DATA
def clean_and_filter_transactions(df: pd.DataFrame, start_date: str, end_date: str, customer_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and filters transaction data based on specified date range and customer information.

    This function takes a DataFrame of transactions and filters it based on a given date
    range and a DataFrame of valid customer IDs. It first applies a cleaning and filtering
    step based on the date parameters and then removes any rows from the transaction data
    that do not correspond to customers in the provided customer DataFrame.

    Args:
        df (pd.DataFrame): The transaction data to be cleaned and filtered.
        start_date (str): The start date of the filtering range in the format 'YYYY-MM-DD'.
        end_date (str): The end date of the filtering range in the format 'YYYY-MM-DD'.
        customer_df (pd.DataFrame): A DataFrame containing valid customer information, 
            including the column `customer_id`.

    Returns:
        pd.DataFrame: The filtered DataFrame containing cleaned transaction data.
    """

    # drop 'links' column (Netsuite inserts this)
    df = df.drop('links', axis=1)

    # remove all rows outside the date range
    df = df[(df["created_date"] >= start_date) & (df["created_date"] <= end_date)]

    # drop rows with NaT created_date
    df = df[~df["created_date"].isna()]
    
    # drop rows with customer IDs that are not in the customer data
    df = df[df["customer_id"].isin(customer_df["customer_id"])]
    
    return df


def clean_and_filter_line_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and filters line item DataFrame based on specified conditions for use in
    further processing. The function removes unnecessary data, and filters relevant
    records.

    Args:
        df (pd.DataFrame): Input DataFrame containing line item data to be
            cleaned and filtered.

    Returns:
        pd.DataFrame: A cleaned and filtered DataFrame with irrelevant records and
        unused fields removed.
    """

    # drop 'links' column (Netsuite inserts this)
    df = df.drop('links', axis=1)

    # drop df with both cost and unit_price <= 0
    df = df.query('quote_po_rate > 0 or unit_price > 0')
    
    # drop item types that are not related to products/services
    drop_list = ["Description", "Markup", "Other Charge", "Payment", "Discount"]
    df = df[~df["item_type"].isin(drop_list)]
    
    return df


# AUGMENT DATA
def augment_rows(df: pd.DataFrame, customer_df: pd.DataFrame, location_map: dict) -> pd.DataFrame:
    """
    Augments a dataframe by merging customer-related information and matching subsidiary
    information based on location.

    The function merges the input dataframe (`df`) with customer data (`customer_df`) to
    enrich it with customer-specific details such as company name, subsidiary name, end
    market, and sales representative. Additionally, it updates the subsidiary information
    by matching locations in the line df using the provided location map.

    Args:
        df (pd.DataFrame): A dataframe containing the initial data to be augmented.
        customer_df (pd.DataFrame): A dataframe with customer-related information including
            fields like customer_id, company_name, subsidiary_name, end_market, and sales_rep.
        location_map (dict): A dictionary defining location mappings used to set subsidiaries
            for the relevant line df.

    Returns:
        pd.DataFrame: The augmented dataframe with additional customer and subsidiary fields.
    """
    # add customer info
    df = df.merge(
        customer_df[["customer_id", "company_name", "subsidiary_name", "end_market", "sales_rep"]],
        on="customer_id",
        how="left")

    # join is creating NaN in subsidiary_name a few rows where customer_id's must not be in customers
    # even though I am dropping all rows that aren't in customer table ?????
    df['subsidiary_name'] = df['subsidiary_name'].fillna('Not Specified').astype(str)

    # match subsidiary by location in each line item, because they may be different
    df = dm.set_subsidiary_by_location(df, location_map)

    # See all Python types present
    print(df['subsidiary_name'].map(type).value_counts())

    return df


def augment_transactions(df: pd.DataFrame, customer_df: pd.DataFrame, location_map: dict) -> pd.DataFrame:
    """
    Augments transaction data by enriching it with additional details such as customer
    information and mapped location data.

    This function takes a primary DataFrame containing transactions and augments it
    using additional data frames and mappings. The purpose is to enhance the transaction
    records with relevant details extracted or inferred from auxiliary sources.

    Args:
        df: DataFrame containing the base transaction data to be augmented.
        customer_df: DataFrame containing customer details to enrich the transaction data.
        location_map: Dictionary mapping location identifiers to their detailed descriptions.

    Returns:
        DataFrame: Augmented DataFrame with additional information added to transaction records.
    """
    return augment_rows(df, customer_df,location_map)


def get_highest_recent_prices(
    line_items: pd.DataFrame,
    purchase_orders: pd.DataFrame,
    sku_col: str = 'sku',
    date_col: str = 'created_date',
    price_col: str = 'unit_price',
    window: Literal['365D'] = '365D',
    output_col: str = 'highest_recent_cost'
) -> pd.DataFrame:
    """Annotate each line-item with the max purchase price for the same SKU over
    the window ending at its created_date.

    Args:
        line_items: DataFrame with [sku_col, date_col].
        purchase_orders: DataFrame with [sku_col, date_col, price_col].
        sku_col: column name for SKU.
        date_col: column name for the date.
        price_col: column name for purchase price.
        window: rolling window length (e.g. '365D').
        output_col: name of the output column.

    Returns:
        A copy of `line_items` with an extra `output_col`.
    """
    # ---- 1) Prep and sort ----
    li = line_items.copy()
    po = purchase_orders.copy()
    li[date_col] = pd.to_datetime(li[date_col])
    po[date_col] = pd.to_datetime(po[date_col])

    li['_orig_idx'] = li.index
    li_sorted = li.sort_values(date_col)

    po_sorted = po.sort_values([sku_col, date_col])

    # ---- 2) Compute exact 365-day rolling max per PO date ----
    rolling = (
        po_sorted
          .set_index(date_col)
          .groupby(sku_col)[price_col]
          .rolling(window)
          .max()
          .reset_index()
          .rename(columns={price_col: output_col})
    )
    # ensure sorted by date for merge_asof
    rolling.sort_values(date_col, inplace=True)

    # ---- 3) As-of merge onto line df ----
    merged = pd.merge_asof(
        li_sorted,
        rolling,
        on=date_col,
        by=sku_col,
        direction='backward'
    )

    # ---- 4) Restore original order ----
    result = (
        merged
          .sort_values('_orig_idx')
          .drop(columns=['_orig_idx'])
          .reset_index(drop=True)
    )
    return result.fillna(0.0)


def augment_line_items(line_item_df: pd.DataFrame,
                       transaction_df: pd.DataFrame,
                       item_master_df: pd.DataFrame,
                       purchase_order_df: pd.DataFrame,
                       customer_df: pd.DataFrame,
                       location_map: dict,
                       start_date: str,
                       end_date: str) -> pd.DataFrame:
    """
    Augments the line df DataFrame with additional details from related DataFrames such as transaction
    data, customer data, and item master data. It also performs data cleaning, feature engineering, and
    financial calculations at the line item level.

    Args:
        line_item_df (pd.DataFrame): The line df DataFrame to be augmented.
        transaction_df (pd.DataFrame): A DataFrame containing transaction-level details, including transaction
            IDs, creation dates, and order type.
        item_master_df (pd.DataFrame): A DataFrame containing item master details such as SKU, manufacturer,
            categories, and other attributes for df.
        purchase_order_df: A DataFrame containing the purchase order line df
        customer_df: A DataFrame containing information about customers, which is used to enrich line item data
            along with the location map.
        location_map (dict): A mapping of locations used to map and enrich customer-related information for
            corresponding line df.

    Returns:
        pd.DataFrame: The augmented line df DataFrame including integrated details from transactions,
            customers, and item master records, as well as computed financial metrics.
    """

    # add transaction info to line_items
    trans_level_cols = ["tranid", "created_date", 'commission_only', 'ai_order_type', 'entered_by']
    line_item_df = line_item_df.merge(transaction_df[trans_level_cols], on="tranid", how="left")

    # remove all rows outside the date range
    line_item_df = line_item_df[(line_item_df["created_date"] >= start_date) & (line_item_df["created_date"] <= end_date)]

    # drop rows with NaT created_date
    line_item_df = line_item_df[~line_item_df["created_date"].isna()]

    # drop rows with customer IDs that are not in the customer data
    line_item_df = line_item_df[line_item_df["customer_id"].isin(customer_df["customer_id"])]

    # add customer info
    line_item_df = augment_rows(line_item_df, customer_df, location_map)

    # create new column to combine commission fields and drop the old one
    line_item_df["commission_or_mfr_direct"] = (
            (line_item_df["commission_only"] == True) |
            (line_item_df["ai_order_type"].isin(["Commission Order", "Manufacturer Direct"]))
    )
    line_item_df.drop("commission_only", axis=1, inplace=True)

    # add category info
    line_item_df = dm.add_item_master_fields(line_item_df, item_master_df)

    # move any valid manufacturer into the manufacturer field from custom or vsi fields
    line_item_df = clean_and_resolve_manufacturers(line_item_df)

    # calculate financial values for each line item
    line_item_df["total_amount"] = line_item_df["quantity"] * line_item_df["unit_price"]
    line_item_df["total_cost"] = line_item_df["quantity"] * line_item_df["quote_po_rate"]
    line_item_df["gross_profit"] = line_item_df["total_amount"] - line_item_df["total_cost"]
    line_item_df["gross_profit_percent"] = line_item_df["gross_profit"] / line_item_df["total_amount"]

    # find the highest purchase cost and highest quoted cost for every item using
    # purchase order and line item data over a 12-month rolling window
    line_item_df = get_highest_recent_prices(line_item_df, purchase_order_df, output_col='highest_recent_cost')
    line_item_df = get_highest_recent_prices(line_item_df, line_item_df,
                                             price_col='quote_po_rate', output_col='highest_quoted_cost')

    # create a highest_cost column by taking the max of the two cost columns
    line_item_df['highest_cost'] = line_item_df[['highest_quoted_cost', 'highest_recent_cost']].max(axis=1)

    # round floats to two decimals again
    line_item_df = round_float_columns(line_item_df)

    return line_item_df


# MAIN
def main() -> None:
    """
    Main function to process, repair, clean, and augment transaction data from a data lake based on specified
    transaction types. The processed data is then saved back into the data lake in both cleaned and enhanced forms.

    This script connects to the Azure Data Lake, retrieves customer and item master data, and processes transaction
    data for specified or all transaction types. It performs the following operations for each transaction type:
    - Retrieves transaction and line item data.
    - Repairs and cleans the data.
    - Filters the data based on a date range.
    - Saves the cleaned data into the data lake.
    - Augments the data with additional details using customer, item master data, and configuration mappings.
    - Saves the augmented data back into the data lake.

    Args:
        --trans-type (str, optional): Specify the transaction type to process. Valid values are 'Estimate', 'SalesOrd', and
        'CustInvc'. If not provided, all transaction types will be processed.

    Raises:
        SystemExit: Raised if invalid or missing arguments are passed to the script.

    """

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process transaction data.')
    parser.add_argument('--trans-type', type=str, choices=['Estimate', 'SalesOrd', 'CustInvc'],
                        help='Transaction type to process. If not specified, all types will be processed.')
    args = parser.parse_args()

    print(f"\rAttaching to data lake...")
    config = load_config("common/config/datalake_config.json", flush_cache=True)
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    print(f"\rGetting customer data from data lake...")
    customers = adl.get_parquet_file_from_data_lake(file_system_client, "cleaned/netsuite", "customer_cleaned.parquet")

    print(f"\rGetting item master data from data lake...")
    items = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite", "item_enhanced.parquet")

    print(f"\rGetting purchase order data from data lake...")
    po_lines = adl.get_parquet_file_from_data_lake(file_system_client, "cleaned/netsuite",
                                                   f"transaction/PurchOrdItemLineItems_cleaned.parquet")

    # set date range
    start_date = "2022-01-01"
    end_date = datetime.date.today().strftime("%Y-%m-%d")

    # Define transaction types
    transaction_types = ["Estimate", "SalesOrd", "CustInvc"]

    # get transaction-level and line item data
    trans_types_to_process = [args.trans_type] if args.trans_type else transaction_types
    for trans_type in trans_types_to_process:
        print(f"Getting {trans_type} transactions and line items from data lake...")
        transactions, line_items = adl.get_transactions_and_line_items(file_system_client, trans_type)

        print(f"Repairing {trans_type} transactions and line items...")
        transactions = repair_transactions(transactions)
        line_items = repair_line_items(line_items)

        print(f"Cleaning and filtering {trans_type} transactions and line items...")
        transactions = clean_and_filter_transactions(transactions, start_date, end_date, customers)
        line_items = clean_and_filter_line_items(line_items)

        print(f"\rSaving cleaned and filtered {trans_type} transactions and line items in data lake...")
        adl.save_df_as_parquet_in_data_lake(transactions, file_system_client, "cleaned/netsuite",
                                            f"transaction/{trans_type}_cleaned.parquet")
        adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, "cleaned/netsuite",
                                            f"transaction/{trans_type}ItemLineItems_cleaned.parquet")

        print(f"\rAugmenting {trans_type} transactions and line items...")
        config = load_config("common/config/location_subsidiary_map.json", flush_cache=True)
        transactions = augment_transactions(transactions, customers, config["locations_subsidiary_map"])
        line_items = augment_line_items(line_items, transactions,items, po_lines,
                                        customers, config["locations_subsidiary_map"], start_date, end_date)

        print(f"\rSaving augmented {trans_type} transactions and line items in data lake...")
        adl.save_df_as_parquet_in_data_lake(transactions, file_system_client, "enhanced/netsuite",
                                            f"transaction/{trans_type}_enhanced.parquet")
        adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, "enhanced/netsuite",
                                            f"transaction/{trans_type}ItemLineItems_enhanced.parquet")


if __name__ == "__main__":
    main()
