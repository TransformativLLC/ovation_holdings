# IMPORTS
# standard libraries
import argparse

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data repair libraries
from common.utils.data_repair import repair_dataframe_data

# config file reader
import common.config
from common.utils.configuration_management import load_config


# MAIN
def main() -> None:
    """
    Main function entry point for repairing raw data in parquet files by processing specified data tables
    or defaulting to all available tables. The function connects to a data lake, retrieves data, repairs the
    data, and saves the processed files back to the data lake.

    The function uses Azure data lake services for handling parquet files, and operations are configured via
    external JSON configurations. It supports filtering specific tables or using predefined groups, such as
    'transactions' and 'other'.

    Args:
        tables (list[str], optional): Command-line argument specifying tables to process. Defaults to all tables
            if not provided. Acceptable values: individual table names or predefined sets ('transactions', 'other').

    Raises:
        ValueError: If invalid table names are provided.
    """

    # set default lists for data tables
    customer_facing_transactions = ["Estimate", "SalesOrd", "CustInvc"]
    purchase_orders = ["PurchOrd"]
    other = ["customer", "vendor", "item"]

    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    # parse arguments, if none passed, default to all
    parser = argparse.ArgumentParser(
        description="Repair raw data in parquet files and save back to raw directory."
    )
    parser.add_argument(
        "tables",
        metavar="TABLES",
        nargs="*",
        default=None,
        help="List of tables to repair, defaults to all tables, can use 'transactions' "
             "or 'other' for pre-defined lists of tables.",
    )

    args = parser.parse_args()

    if args.tables:
        data_to_convert = args.tables
    else:
        all = customer_facing_transactions + purchase_orders + other
        data_to_convert = all

    # read field map
    table_fields_map = load_config(common.config, "table_field_types.json")

    for raw_data_table in data_to_convert:
        match raw_data_table:
            case raw_data_table if raw_data_table in other:
                print(f"Retrieving raw {raw_data_table} data...")
                df = adl.get_parquet_file_from_data_lake(file_system_client, "raw/netsuite",
                                                         f"{raw_data_table}_raw.parquet")
                print(f"Repairing {raw_data_table} data...")
                df = repair_dataframe_data(df, raw_data_table, table_fields_map)

                print(f"Saving repaired {raw_data_table} data...")
                adl.save_df_as_parquet_in_data_lake(df, file_system_client, "raw/netsuite",
                                                    f"{raw_data_table}_repaired.parquet")

            case raw_data_table if raw_data_table in customer_facing_transactions:
                print(f"Retrieving raw {raw_data_table} transaction and line item data...")
                transactions, line_items = adl.get_transactions_and_line_items(file_system_client, raw_data_table)

                # some morons are putting alphabetic characters in the tranid field. Have to drop them for everyting to work.
                alphanumeric_mask = transactions['tranid'].astype(str).str.contains(r'^(?=.*[0-9])(?=.*[a-zA-Z])[a-zA-Z0-9]+$',
                                                                          na=False)
                transactions = transactions[~alphanumeric_mask]
                alphanumeric_mask = line_items['tranid'].astype(str).str.contains(r'^(?=.*[0-9])(?=.*[a-zA-Z])[a-zA-Z0-9]+$',
                                                                          na=False)
                line_items = line_items[~alphanumeric_mask]

                # since transactions are the same, using a shared JSON field map
                print(f"Repairing {raw_data_table} transaction and line item data...")
                transactions = repair_dataframe_data(transactions, "cust_facing_transaction", table_fields_map)
                line_items = repair_dataframe_data(line_items, "cust_facing_line_item", table_fields_map)

                print(f"Saving repaired {raw_data_table} transaction and line item data...")
                adl.save_df_as_parquet_in_data_lake(transactions, file_system_client, "raw/netsuite",
                                                    f"transaction/{raw_data_table}_repaired.parquet")
                adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, "raw/netsuite",
                                                    f"transaction/{raw_data_table}ItemLineItems_repaired.parquet")

            case raw_data_table if raw_data_table in purchase_orders:
                print(f"Retrieving raw {raw_data_table} transaction and line item data...")
                transactions, line_items = adl.get_transactions_and_line_items(file_system_client, raw_data_table)

                # since transactions are the same, using a shared JSON field map
                print(f"Repairing {raw_data_table} transaction and line item data...")
                transactions = repair_dataframe_data(transactions, raw_data_table, table_fields_map)
                line_items = repair_dataframe_data(line_items, f"{raw_data_table}_li", table_fields_map)

                print(f"Saving repaired {raw_data_table} transaction and line item data...")
                adl.save_df_as_parquet_in_data_lake(transactions, file_system_client, "raw/netsuite",
                                                    f"transaction/{raw_data_table}_repaired.parquet")
                adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, "raw/netsuite",
                                                    f"transaction/{raw_data_table}ItemLineItems_repaired.parquet")

            
if __name__ == "__main__":
    main()
