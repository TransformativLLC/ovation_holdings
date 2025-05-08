# IMPORTS
# Standard libraries
import datetime

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# Data analysis libraries
import pandas as pd

# Data manipulation libraries
from common.utils.data_modifications import convert_json_strings_to_python_types

# config
from common.utils.configuration_management import load_config


# FUNCTIONS
def clean_and_filter_trans_dataframe(df, start_date, end_date, customers_df):
    # Convert to datetime and float
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    df['net_amount'] = df['net_amount'].astype(float)

    # Filter rows in-place
    df = df[(df['created_date'] >= start_date) & (df['created_date'] <= end_date)]

    # Drop transactions with customer id's that are not in customers table
    df = df[df["customer_id"].isin(customers_df["id"])]

    return df


def clean_and_filter_customer_data(customers: pd.DataFrame, active_cust_ids: set) -> pd.DataFrame:

    # get customers with ids that appear in active_customer_ids
    active_customers = customers[customers['id'].isin(active_cust_ids)].copy()

    # replace null values with something more descriptive
    active_customers.loc[active_customers['company_name'] == 'null', 'company_name'] = 'Unknown'
    active_customers.loc[active_customers['end_market'] == 'null', 'end_market'] = 'Not Assigned'

    # if primary sales rep is null and ai sales rep is null, set primary sales rep to 'Not Assigned'
    active_customers.loc[(active_customers['primary_sales_rep'] == 'null') & (active_customers['ai_sales_rep'] == 'null'), 'primary_sales_rep'] = 'Not Assigned'

    # if either primary sales rep or ai sales rep is not null, set the value of new column 'sales_rep' to the non-null value, but if they are both non null and don't match, set to 'Multiple'
    active_customers['sales_rep'] = 'Not Assigned'
    active_customers.loc[(active_customers['primary_sales_rep'] != 'null') & (active_customers['ai_sales_rep'] == 'null'), 'sales_rep'] = active_customers['primary_sales_rep']
    active_customers.loc[(active_customers['primary_sales_rep'] == 'null') & (active_customers['ai_sales_rep'] != 'null'), 'sales_rep'] = active_customers['ai_sales_rep']
    active_customers.loc[(active_customers['primary_sales_rep'] != 'null') & (active_customers['ai_sales_rep'] != 'null') & (active_customers['primary_sales_rep'] != active_customers['ai_sales_rep']), 'sales_rep'] = 'Multiple'

    # fill in values for category that are 'null'
    active_customers.loc[active_customers['category'] == 'null', 'category'] = 'Not Assigned'

    # rename id to customer_id so it can be joined more easily with transaction data
    active_customers.rename(columns={'id': 'customer_id'}, inplace=True)

    # drop column that are all null
    drop_cols = ['links', 'account_number', 'as_cust_serv_rep', 'control_tech_sales_rep', 'glpc_sales_rep', 'jmi_sales_rep',
                 'promac_sales_rep', 'psi_sales_rep', 'shipping_item', 'url']
    active_customers.drop(drop_cols, axis=1, inplace=True)

    active_customers = convert_json_strings_to_python_types(active_customers)

    return active_customers


# MAIN FUNCTION
def main():

    # attach to the data lake
    print("Attaching to data lake...")
    config = load_config("common/config/datalake_config.json", flush_cache=True)
    service_client = adl.get_azure_service_client(config["blob_url"])

    container_name = "consolidated"
    file_system_client = adl.get_azure_file_system_client(service_client, container_name)

    # get customer data
    print("Getting customer data...")
    source_folder = "raw/netsuite"
    customers = adl.get_parquet_file_from_data_lake(file_system_client, source_folder, "customer_raw.parquet")

    # get transaction data
    print("Getting estimates...")
    estimates = adl.get_parquet_file_from_data_lake(file_system_client, source_folder,
                                                    "transaction/Estimate_raw.parquet")
    print("Getting sales orders...")
    sales_orders = adl.get_parquet_file_from_data_lake(file_system_client, source_folder,
                                                       "transaction/SalesOrd_raw.parquet")
    print("Getting invoices...")
    invoices = adl.get_parquet_file_from_data_lake(file_system_client, source_folder,
                                                   "transaction/CustInvc_raw.parquet")

    # clean and filter all transactions
    print("Cleaning and filtering all tranactions...")
    start_date = '2022-01-01'
    end_date = datetime.date.today().strftime('%Y-%m-%d')

    estimates = clean_and_filter_trans_dataframe(estimates, start_date, end_date, customers)
    sales_orders = clean_and_filter_trans_dataframe(sales_orders, start_date, end_date, customers)
    invoices = clean_and_filter_trans_dataframe(invoices, start_date, end_date, customers)

    # combine customer ids in transaction to id active customers
    active_customer_ids = set(estimates["customer_id"].unique()).union(
        sales_orders["customer_id"].unique(), invoices["customer_id"].unique()
    )

    print("Cleaning and filtering customer data...")
    active_customers = clean_and_filter_customer_data(customers, active_customer_ids)

    # save in the data lake
    print("Saving cleaned customer data in data lake...")
    adl.save_df_as_parquet_in_data_lake(active_customers, file_system_client, "cleaned/netsuite",
                                        "customer_cleaned.parquet")


if __name__ == "__main__":
    main()