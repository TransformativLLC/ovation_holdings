# Standard libraries
import datetime
import argparse
from typing import Literal

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data repair/cleaning libraries
from common.utils.data_repair import smart_fillna
from common.utils.data_cleansing import round_float_columns, set_subsidiary_by_location

# data augmentation libraries
from common.utils.data_augmentation import add_new_category_levels, add_vsi_item_category

# Data analysis libraries
from pandas import DataFrame, Timestamp, to_datetime, merge_asof

# config
import common.config
from common.utils.configuration_management import load_config


# FUNCTIONS
def augment_rows(df: DataFrame, customer_df: DataFrame, location_map: dict) -> DataFrame:
    """
    Adds detailed customer data to the provided DataFrame and matches subsidiary by
    location for each record.

    This function takes an input DataFrame, merges it with detailed customer
    information from `customer_df` based on the `customer_id` column, and enriches
    the primary DataFrame with additional columns like company name, subsidiary
    name, end market, and sales representative. It further processes the data to
    assign the correct subsidiary based on a given location mapping.

    Args:
        df (DataFrame): The primary DataFrame containing transaction or customer
            records that need augmentation. It must include the column
            `customer_id` to enable merging.

        customer_df (DataFrame): A DataFrame containing detailed customer
            information. It should include the columns `customer_id`, `company_name`,
            `subsidiary_name`, `end_market`, and `sales_rep` for merging purposes.

        location_map (dict): A dictionary mapping locations to subsidiary
            information. Used to match and update subsidiary information in the
            input DataFrame based on location details.

    Returns:
        DataFrame: The enriched DataFrame, which includes additional customer
        details and updated subsidiary information based on the location mapping.
    """

    # add customer info
    cols = ["customer_id", "subsidiary_name", "end_market", "sales_rep"]
    if not ("company_name" in df.columns):  # it's in transaction but not line item
        cols.append("company_name")

    df = df.merge(
        customer_df[cols],
        on="customer_id",
        how="left")

    # match subsidiary by location in each record
    df = set_subsidiary_by_location(df, location_map)

    return df


def augment_transactions(df: DataFrame, customer_df: DataFrame, location_map: dict) -> DataFrame:
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
    df = augment_rows(df, customer_df, location_map)

    # joins may create NA if keys don't match, so fill them in
    df = smart_fillna(df)

    return df


def get_highest_recent_prices(
    line_items: DataFrame,
    purchase_orders: DataFrame,
    sku_col: str = 'sku',
    date_col: str = 'created_date',
    price_col: str = 'unit_price',
    window: Literal['365D'] = '365D',
    output_col: str = 'highest_recent_cost'
) -> DataFrame:
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
    li[date_col] = to_datetime(li[date_col])
    po[date_col] = to_datetime(po[date_col])

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
    merged = merge_asof(
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

    return result


def augment_line_items(line_item_df: DataFrame,
                       transaction_df: DataFrame,
                       item_master_df: DataFrame,
                       purchase_order_df: DataFrame,
                       customer_df: DataFrame,
                       location_map: dict,
                       start_date: str,
                       end_date: str) -> DataFrame:
    """
    Augments the line item DataFrame with additional data and calculated fields using
    information from multiple other DataFrames. This process includes merging transaction,
    customer, and category information, calculating financial metrics, and appending
    the highest cost data from recent transactions.

    Args:
        line_item_df (DataFrame): DataFrame containing line item details.
        transaction_df (DataFrame): DataFrame containing transaction-level details.
        item_master_df (DataFrame): DataFrame containing item master information.
        purchase_order_df (DataFrame): DataFrame containing purchase order details.
        customer_df (DataFrame): DataFrame containing customer-related details.
        location_map (dict): Mapping of location-specific data.
        start_date (str): Start date for filtering records. Should be in ISO 8601 format.
        end_date (str): End date for filtering records. Should be in ISO 8601 format.

    Returns:
        DataFrame: Enhanced and augmented DataFrame containing additional information
        and calculated fields for each line item.
    """

    # add transaction info to line_items
    trans_level_cols = ["tranid", "created_date", 'commission_only', 'ai_order_type', 'entered_by']
    line_item_df = line_item_df.merge(transaction_df[trans_level_cols], on="tranid", how="left")
    line_item_df["created_date"] = line_item_df["created_date"].fillna(Timestamp("1800-01-01"))

    # remove all rows outside the date range
    line_item_df = line_item_df[(line_item_df["created_date"] >= start_date) & (line_item_df["created_date"] <= end_date)]

    # add customer info
    line_item_df = augment_rows(line_item_df, customer_df, location_map)

    # create new column to combine commission fields and drop the old one
    line_item_df["commission_or_mfr_direct"] = (
            (line_item_df["commission_only"] == True) |
            (line_item_df["ai_order_type"].isin(["Commission Order", "Manufacturer Direct"]))
    )
    line_item_df.drop("commission_only", axis=1, inplace=True)

    # add category info and vsi_category
    line_item_df = add_new_category_levels(line_item_df, item_master_df)
    line_item_df = add_vsi_item_category(line_item_df, item_master_df)

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

    # joins may create NA if keys don't match, so fill them in
    line_item_df = smart_fillna(line_item_df)

    return line_item_df


# MAIN
def main() -> None:
    """
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

    print(f"\rGetting customer data from data lake...")
    customers = adl.get_parquet_file_from_data_lake(file_system_client, "cleaned/netsuite", "customer_cleaned.parquet")

    print(f"\rGetting item master data from data lake...")
    items = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite", "item_enhanced.parquet")

    print(f"\rGetting purchase order data from data lake...")
    po_lines = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite",
                                                   f"transaction/PurchOrdItemLineItems_enhanced.parquet")

    # set date range
    start_date = "2022-01-01"
    end_date = datetime.date.today().strftime("%Y-%m-%d")

    # Define transaction types
    transaction_types = ["Estimate", "SalesOrd", "CustInvc"]

    # get transaction-level and line item data
    trans_types_to_process = [args.trans_type] if args.trans_type else transaction_types
    for trans_type in trans_types_to_process:
        print(f"Getting {trans_type} transactions and line items from data lake...")
        data_state = "cleaned"
        transactions = adl.get_parquet_file_from_data_lake(file_system_client, f"{data_state}/netsuite",
                                                           f"transaction/{trans_type}_{data_state}.parquet")
        line_items = adl.get_parquet_file_from_data_lake(file_system_client, f"{data_state}/netsuite",
                                                         f"transaction/{trans_type}ItemLineItems_{data_state}.parquet")

        print(f"Augmenting {trans_type} transactions and line items...")
        config = load_config(common.config, "location_subsidiary_map.json")
        transactions = augment_transactions(transactions, customers, config["locations_subsidiary_map"])
        line_items = augment_line_items(line_items, transactions,items, po_lines,
                                        customers, config["locations_subsidiary_map"], start_date, end_date)

        print(f"Saving augmented {trans_type} transactions and line items in data lake...")
        adl.save_df_as_parquet_in_data_lake(transactions, file_system_client, "enhanced/netsuite",
                                            f"transaction/{trans_type}_enhanced.parquet")
        adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, "enhanced/netsuite",
                                            f"transaction/{trans_type}ItemLineItems_enhanced.parquet")


if __name__ == "__main__":
    main()
