# IMPORTS
# standard libraries
import argparse

# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# config file reader
import common.config
from common.utils.configuration_management import load_config


# MAIN
def main() -> None:
    """
    Main function to consolidate individual JSON records from specified tables into a Parquet file.

    This function connects to a specified data lake, processes tables (either passed as arguments or
    default sets), and consolidates their JSON data into Parquet format by invoking a conversion function.
    It can handle both transaction and non-transaction tables, ensuring that data is structured correctly.
    """

    # set default lists for data tables
    transactions = ["Estimate", "SalesOrd", "CustInvc", "PurchOrd"]
    other = ["customer", "vendor", "item"]

    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])

    # parse arguments, if none passed, default to all
    parser = argparse.ArgumentParser(
        description="Consoldiate individual JSON records into a Parquet file."
    )
    parser.add_argument(
        "tables",
        metavar="TABLES",
        nargs="*",
        default=None,
        help="List of tables to consolidate, defaults to all tables, can use 'transactions' "
             "or 'other' for pre-defined lists of tables.",
    )

    args = parser.parse_args()

    if args.tables:
        data_to_convert = args.tables
    else:
        all = transactions + other
        data_to_convert = all

    for json_source in data_to_convert:
        if json_source in other:
            print(f"Converting {json_source} data...")
            adl.convert_json_to_parquet(service_client, "netsuite", json_source)
        else:
            print(f"Converting {json_source} transaction data...")
            adl.convert_json_to_parquet(service_client, "netsuite", f"transaction/{json_source}")

            print(f"Converting {json_source} line item data...")
            adl.convert_json_to_parquet(service_client, "netsuite", f"transaction/{json_source}ItemLineItems")
            
            
if __name__ == "__main__":
    main()
