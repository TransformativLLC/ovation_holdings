# IMPORTS
# Azure Data Lake libraries
import common.utils.azure_data_lake_interface as adl

# data augmentation libraries
from common.utils.data_augmentation import add_new_category_levels, add_vsi_item_category

# Data analysis libraries
from pandas import DataFrame, Timestamp

# config
import common.config
from common.utils.configuration_management import load_config


# FUNCTIONS
def augment_po_line_items(line_items: DataFrame, transactions: DataFrame, items: DataFrame, vendors: DataFrame) -> DataFrame:
    """
    """

    # add created date to line items
    line_items = line_items.merge(transactions[["tranid", "created_date"]], on="tranid", how="left")
    line_items["created_date"] = line_items["created_date"].fillna(Timestamp("1800-01-01"))

    # update/add category level info
    line_items = add_new_category_levels(line_items, items)

    # add vsi_category
    line_items = add_vsi_item_category(line_items, items)

    # add vendor information to line_items after changing col name
    vendors.rename(columns={"id": "vendor_id"}, inplace=True)
    line_items = line_items.merge(vendors[["vendor_id", "company_name", "category"]], on="vendor_id", how="left")

    # calculate total amount for each line item
    line_items["total_amount"] = line_items["quantity"] * line_items["unit_price"]

    return line_items


# MAIN
def main():

    print("Attaching to data lake...")
    config = load_config(common.config, "datalake_config.json")
    service_client = adl.get_azure_service_client(config["blob_url"])
    file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")

    print(f"Getting purchase order transactions and line items from data lake...")
    trans_type = "PurchOrd"
    data_state = "cleaned"
    transactions = adl.get_parquet_file_from_data_lake(file_system_client, f"{data_state}/netsuite",
                                                       f"transaction/{trans_type}_{data_state}.parquet")
    line_items = adl.get_parquet_file_from_data_lake(file_system_client, f"{data_state}/netsuite",
                                                     f"transaction/{trans_type}ItemLineItems_{data_state}.parquet")

    print("Getting vendor and item master data...")
    vendors = adl.get_parquet_file_from_data_lake(file_system_client, "cleaned/netsuite", "vendor_cleaned.parquet")
    items = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite", "item_enhanced.parquet")

    print("Enhancing purchase order line item data...")
    line_items = augment_po_line_items(line_items, transactions, items, vendors)

    data_state = "enhanced"
    print("Saving enhanced purchase order line df in data lake...")
    adl.save_df_as_parquet_in_data_lake(line_items, file_system_client, f"{data_state}/netsuite",
                                        f"transaction/{trans_type}ItemLineItems_{data_state}.parquet")


if __name__ == "__main__":
    main()