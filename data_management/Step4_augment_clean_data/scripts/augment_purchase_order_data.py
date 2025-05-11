# IMPORTS

# Standard libraries
import datetime
from typing import Tuple

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# Data analysis libraries
import pandas as pd

# config
import common.config
from common.utils.configuration_management import load_config


# FUNCTIONS
def clean_filter_augment_purchase_orders(transactions: pd.DataFrame,
                                          line_items: pd.DataFrame,
                                          items: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans, filters, and augments purchase orders by processing transactions and line df data.
    This function applies transformations such as date range filtering, removing irrelevant data, dropping
    non-essential columns, converting data types, and incorporating additional data from vendor and item
    sources. The resulting datasets are refined to better reflect purchase order data.

    Args:
        transactions (pd.DataFrame): DataFrame containing transaction records.
        line_items (pd.DataFrame): DataFrame containing line item records associated with transactions.
        vendors (pd.DataFrame): DataFrame containing vendor information.
        items (pd.DataFrame): DataFrame of item records, including product or service details.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple where the first element is the cleaned and transformed
            transactions DataFrame, and the second element is the cleaned and augmented line df DataFrame.

    """

    # most columns in the transactions don't make sense for purchase orders, so drop them
    drop_columns = [
        'links', 'actual_ship_date', 'ai_order_type', 'amount_paid', 'amount_unpaid', 'billing_address',
        'close_date', 'commission_only', 'company_email', 'created_by', 'custom_form', 'date_started',
        'days_open', 'deliver_by_date', 'due_date', 'employee', 'end_date', 'entered_by', 'entity_status',
        'estimated_gross_profit', 'estimated_gross_profit_percent', 'expected_close_date', 'finance_charge',
        'freshdesk_ticket_number', 'inbound_source', 'job_type', 'last_modified_by', 'lastmodifieddate',
        'lead_source', 'mainline', 'memo', 'nexus', 'posting_period', 'prepared_for_contact',
        'prepared_for_contact_email', 'promised_date', 'reversal', 'ship_date', 'shipping_address', 'start_date',
        'type', 'voided', 'nx_customer_id', 'vsi_service_type'
    ]
    transactions = transactions.drop(columns=drop_columns)

    # similarly, most line item columns don't make sense for purchase orders, so drop them
    drop_columns = [
        'links', 'amount', 'assembly_component', 'cost_estimate_type', 'created_from', 'custcol_ava_taxamount',
        'custcol_sa_quote_po_rate', 'est_extended_cost', 'est_gross_profit', 'est_gross_profit_percent',
        'handling_cost', 'item_base_price', 'labor_hours', 'line_number', 'mainline',
        'nx_customer_id', 'quote_po_rate', 'special_order', 'tax_line', 'transaction_table_id', 'valve_spec_size',
        'vendor_commission_percent'
    ]
    line_items = line_items.drop(columns=drop_columns)

    transactions = convert_json_strings_to_python_types(transactions)
    line_items = convert_json_strings_to_python_types(line_items)

    # add created date to line df
    line_items = line_items.merge(transactions[["tranid", "created_date"]], on="tranid", how="left")

    # drop item types that are not related to products/services
    drop_list = ["Description", "Markup", "Other Charge", "Payment", "Discount"]
    line_items = line_items[~line_items["item_type"].isin(drop_list)]

    # Replace null values with replacement_value
    transactions["location"] = transactions["location"].replace("null", "Not Specified")
    line_items["location"] = line_items["location"].replace("null", "Not Specified")

    # replace values in line df with values from item master (levels and manufacturer)
    line_items = add_category_levels_and_vsi_info(line_items, items)

    # calculate total amount for each line item
    line_items["total_amount"] = line_items["quantity"] * line_items["unit_price"]

    # round floats to two decimals
    transactions = round_float_columns(transactions)
    line_items = round_float_columns(line_items)

    return transactions, line_items


# MAIN
def main():

    # attach to the data lake
    print("Attaching to data lake...")
    config = load_config("common/config/datalake_config.json", flush_cache=True)
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    # get transaction-level and line item data
    print("Getting purchase order data...")
    trans_type = "PurchOrd"
    transactions, line_items = adl.get_transactions_and_line_items(file_system_client, trans_type)

    # get vendor data
    print("Getting vendor and item master data...")
    vendors = adl.get_parquet_file_from_data_lake(file_system_client, "cleaned/netsuite", "vendor_cleaned.parquet")
    items = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite", "item_enhanced.parquet")

    start_date = "2022-01-01"
    end_date = datetime.date.today().strftime("%Y-%m-%d")

    print("Cleaning purchase order data...")
    transactions, line_items = clean_filter_augment_purchase_orders(transactions, line_items, items)

    # save in data lake
    print("Saving cleaned purchase order data in data lake...")
    adl.save_df_as_parquet_in_data_lake(transactions, file_system_client, "cleaned/netsuite",
                                        f"transaction/{trans_type}_cleaned.parquet")
    adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, "cleaned/netsuite",
                                        f"transaction/{trans_type}ItemLineItems_cleaned.parquet")

    # I should put this in its own script, but seems inefficient to do so
    print("Enhanceing purchase order line item data...")
    # add vendor information to line_items after changing col name
    vendors.rename(columns={"id": "vendor_id"}, inplace=True)
    augmented_line_items = line_items.merge(vendors[["vendor_id", "company_name", "category"]], on="vendor_id",
                                            how="left")

    # save in the data lake
    print("Saving enhanced purchase order line df in data lake...")
    adl.save_df_as_parquet_in_data_lake(augmented_line_items, file_system_client, "enhanced/netsuite",
                                        f"transaction/{trans_type}ItemLineItems_enhanced.parquet")


if __name__ == "__main__":
    main()