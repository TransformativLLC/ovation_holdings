# IMPORTS
# Standard libraries
import datetime

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data cleaning libraries
from common.utils.data_cleansing import drop_dataframe_columns

# Data analysis libraries
import pandas as pd

# config
import common.config
from common.utils.configuration_management import load_config


# FUNCTIONS
def filter_trans_dataframe(df, start_date, end_date):
    """
    Filters a DataFrame to include rows with dates within the specified range.

    This function takes a DataFrame containing a 'created_date' column and filters its rows
    to include only those with 'created_date' values between the `start_date` and `end_date`,
    inclusive. The returned DataFrame will have the same structure as the input DataFrame
    but may have fewer rows.

    Args:
        df: A pandas DataFrame to be filtered. It should contain a 'created_date' column
            with datetime-compatible values.
        start_date: The starting date of the range, inclusive.
        end_date: The ending date of the range, inclusive.

    Returns:
        A pandas DataFrame containing rows from the input DataFrame where the 'created_date'
        is within the specified date range.
    """

    # Filter to the specified date range
    df = df[(df['created_date'] >= start_date) & (df['created_date'] <= end_date)]

    return df


def clean_and_filter_customer_data(customers: pd.DataFrame, active_cust_ids: set,
                                   default_str_for_na: str = "Not Specified") -> pd.DataFrame:
    """
    Cleans and filters customer data by applying specific rules to the input DataFrame,
    such as filtering on active customer IDs, resolving sales representative conflicts,
    and renaming columns. The function also drops unnecessary columns as defined in the
    external configuration.

    Args:
        customers (pd.DataFrame): The input DataFrame containing customer data, including 'id',
            'primary_sales_rep', and 'ai_sales_rep' columns.
        active_cust_ids (set): A set of customer IDs that are considered active and will
            be retained in the filtered DataFrame.
        default_str_for_na (str, optional): The default placeholder for missing or
            unspecified string values. Defaults to "Not Specified".

    Returns:
        pd.DataFrame: A DataFrame containing only active customers, with processed 'sales_rep'
            information, a renamed 'customer_id' column, and excluded unused columns.
    """
    # get customers with ids that appear in active_customer_ids
    active_customers = customers[customers['id'].isin(active_cust_ids)].copy()

    # if either primary sales rep or ai sales rep is not null, set the value of new column 'sales_rep' to
    # the non-null value, but if they are both non null and don't match, set to 'Multiple'
    active_customers['sales_rep'] = default_str_for_na
    active_customers['sales_rep'] = active_customers['sales_rep'].astype("string")
    active_customers.loc[(active_customers['primary_sales_rep'] != default_str_for_na) & (active_customers['ai_sales_rep'] == default_str_for_na), 'sales_rep'] = active_customers['primary_sales_rep']
    active_customers.loc[(active_customers['primary_sales_rep'] == default_str_for_na) & (active_customers['ai_sales_rep'] != default_str_for_na), 'sales_rep'] = active_customers['ai_sales_rep']
    active_customers.loc[(active_customers['primary_sales_rep'] != default_str_for_na) & (active_customers['ai_sales_rep'] != default_str_for_na) & (active_customers['primary_sales_rep'] != active_customers['ai_sales_rep']), 'sales_rep'] = 'Multiple'

    # rename id to customer_id so it can be joined more easily with transaction data
    active_customers.rename(columns={'id': 'customer_id'}, inplace=True)

    # drop columns that are not used
    active_customers = drop_dataframe_columns(active_customers, "customer")

    return active_customers


# MAIN FUNCTION
def main():

    # attach to the data lake
    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])

    container_name = "consolidated"
    file_system_client = adl.get_azure_file_system_client(service_client, container_name)

    # get customer data
    print("Getting customer data...")
    source_folder = "raw/netsuite"
    customers = adl.get_parquet_file_from_data_lake(file_system_client, source_folder, "customer_repaired.parquet")

    # get transaction data
    print("Getting estimates...")
    estimates = adl.get_parquet_file_from_data_lake(file_system_client, source_folder,"transaction/Estimate_repaired.parquet")
    print("Getting sales orders...")
    sales_orders = adl.get_parquet_file_from_data_lake(file_system_client, source_folder,"transaction/SalesOrd_repaired.parquet")
    print("Getting invoices...")
    invoices = adl.get_parquet_file_from_data_lake(file_system_client, source_folder,"transaction/CustInvc_repaired.parquet")

    # clean and filter all transactions
    print("Cleaning and filtering all tranactions...")
    start_date = '2022-01-01'
    end_date = datetime.date.today().strftime('%Y-%m-%d')

    estimates = filter_trans_dataframe(estimates, start_date, end_date)
    sales_orders = filter_trans_dataframe(sales_orders, start_date, end_date)
    invoices = filter_trans_dataframe(invoices, start_date, end_date)

    # combine customer ids in transaction to id active customers
    active_customer_ids = set(estimates["customer_id"].unique()).union(
        sales_orders["customer_id"].unique(), invoices["customer_id"].unique()
    )

    print("Cleaning customer data...")
    active_customers = clean_and_filter_customer_data(customers, active_customer_ids)

    # save in the data lake
    print("Saving cleaned customer data in data lake...")
    adl.save_df_as_parquet_in_data_lake(active_customers, file_system_client, "cleaned/netsuite",
                                        "customer_cleaned.parquet")


if __name__ == "__main__":
    main()