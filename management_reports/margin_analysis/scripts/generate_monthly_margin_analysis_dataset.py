# add path to custom python code for accessing data lake and working with dataframes
import sys
sys.path.append('/Users/markbills/Library/CloudStorage/OneDrive-Transformativ,LLC/Clients/Ovation Holdings/src')

# IMPORT LIBRARIES
# Azure Data Lake libraries
import azure_data_lake_interface as adl

# Helper function libraries
import helper_functions as hf

# data analysis
import pandas as pd


# DATA ENHANCEMENT FUNCTIONS
def get_monthly_item_costs(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates monthly cost metrics for items from purchase order data.

    Processes purchase order data to calculate various monthly cost metrics including
    quantity purchased, total spend, number of PO lines, and unit costs. Also computes
    a rolling maximum unit cost over the specified number of months.

    Args:
        df: DataFrame containing purchase order line items with columns:
            created_date, item_name, manufacturer, description, item_type,
            quantity, total_amount, unit_price
        months: Number of months to use for calculating rolling maximum unit cost.
            Defaults to 12.

    Returns:
        DataFrame indexed by month and item_name containing monthly cost metrics:
            - manufacturer: Item manufacturer
            - description: Item description
            - item_type: Type of item
            - monthly_quantity_purchased: Total quantity purchased that month
            - monthly_spend: Total amount spent that month
            - num_po_lines: Number of PO lines that month
            - highest_unit_cost: Maximum unit cost that month
            - avg_unit_cost: Average unit cost that month
            - {months}_month_max_unit_cost: Rolling maximum unit cost over specified months
    """
    # create a month column for grouping
    df['month'] = df['created_date'].dt.to_period('M')

    # Move the "month" column to the first position
    cols = list(df.columns)
    cols.remove('month')
    df = df[['month'] + cols]

    monthly_data = df.groupby(["month", 'sku']).agg(
        manufacturer=("manufacturer", "first"),
        item_name=("item_name", "first"),
        description=("description", "first"),
        item_type=("item_type", "first"),
        monthly_quantity_purchased=('quantity', 'sum'),
        monthly_spend=('total_amount', 'sum'),
        highest_unit_cost=("unit_price", "max"),
    ).assign(
        avg_unit_cost=lambda df: df['monthly_spend'] / df['monthly_quantity_purchased']
    )

    # Ensure the DataFrame is sorted by the "month" index level
    monthly_data = monthly_data.sort_index(level='month')

    return monthly_data


def get_monthly_item_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates monthly price metrics for items from invoice data.

    Processes invoice line item data to calculate various monthly price metrics including
    quantity sold, total sales, number of invoice lines, and average unit prices.

    Args:
        df: DataFrame containing invoice line items with columns:
            created_date, sku, manufacturer, description, item_type,
            quantity, total_amount

    Returns:
        DataFrame indexed by month and sku containing monthly price metrics:
            - manufacturer: Item manufacturer
            - description: Item description
            - item_type: Type of item
            - monthly_quantity_sold: Total quantity sold that month
            - monthly_sales: Total sales amount that month
            - num_invoice_lines: Number of invoice lines that month
            - avg_unit_price: Average unit price that month
    """

    # create a month column for grouping
    df = df.copy()
    df.loc[:, 'month'] = df['created_date'].dt.to_period('M')

    # Move the "month" column to the first position
    cols = list(df.columns)
    cols.remove('month')
    df = df[['month'] + cols]

    # separate inventory sales and commission sales
    # 1) create a commission mask
    commission_mask = (df["commission_or_mfr_direct"] == True)

    # 2) create columns based on that mask
    df = df.assign(
        commission_sales=df["total_amount"].where(commission_mask, 0),
        commission_qty=df["quantity"].where(commission_mask, 0),
        inventory_sales=df["total_amount"].where(~commission_mask, 0),
        inventory_qty=df["quantity"].where(~commission_mask, 0),
    )

    # 2) group on month & sku, calculate sums and averages
    monthly_data = (
        df.groupby(['month', 'sku']).agg(
            manufacturer=('manufacturer', 'first'),
            description=('description', 'first'),
            item_name=('item_name', 'first'),
            item_type=('item_type', 'first'),
            avg_quoted_cost=('quote_po_rate', 'mean'),
            monthly_inventory_sales=('inventory_sales', 'sum'),
            monthly_inventory_qty=('inventory_qty', 'sum'),
            monthly_commission_sales=('commission_sales', 'sum'),
            monthly_commission_qty=('commission_qty', 'sum'),
            highest_recent_unit_cost=('highest_recent_cost', 'max'),
        ).assign(
            inventory_avg_unit_price=lambda df: df['monthly_inventory_sales'] / df['monthly_inventory_qty'],
            commission_avg_unit_price= lambda df: df['monthly_commission_sales'] / df['monthly_commission_qty']
        )
    )

    return monthly_data.sort_index(level='month')


def combine_and_enhance_line_item_data(po_lines: pd.DataFrame,
                                       trans_lines: pd.DataFrame,
                                       items: pd.DataFrame,
                                       remove_zero_price_lines: bool = True) -> pd.DataFrame:
    """Combines and enhances monthly item price and cost data with category information.

    Processes purchase order and invoice line item data to calculate monthly metrics,
    combines the data, and adds category information. Calculates margin metrics based
    on average unit prices and rolling maximum unit costs.

    Args:
        po_lines: DataFrame containing purchase order line items with columns:
            created_date, sku, manufacturer, description, item_type,
            quantity, total_amount, unit_price
        trans_lines: DataFrame containing transaction line items with columns:
            created_date, sku, manufacturer, description, item_type,
            quantity, total_amount
        items: DataFrame containing item master information
        remove_zero_price_lines: Boolean indicating whether to remove zero price lines from invoice data

    Returns:
        DataFrame indexed by month and sku containing:
            - Item details (description, type, class, manufacturer)
            - Category hierarchy (level 1-6)
            - Sales metrics (quantity sold, sales amount, invoice lines, unit price)
            - Cost metrics (quantity purchased, spend amount, PO lines, unit costs)
            - Margin calculations (average margin amount and percentage)
    """

    # remove trans line items with total amount <= 0 (implying price/quantity problems)
    # because it messes up the margin calculation
    if remove_zero_price_lines:
        trans_lines = trans_lines[trans_lines["total_amount"] > 0]

    # calc monthly costs/prices
    monthly_item_costs = get_monthly_item_costs(po_lines)
    monthly_item_prices = get_monthly_item_prices(trans_lines)

    # Use an outer join to preserve all rows from both dataframes.
    combined_monthly_data = pd.merge(
        monthly_item_prices.reset_index(),
        monthly_item_costs.reset_index(),
        on=['month', 'sku'],
        how='outer',
        suffixes=('_price', '_cost')  # Use suffixes to differentiate duplicate columns
    ).set_index(['month', 'sku'])

    # unify duplicate columns
    for col in ["manufacturer", "item_name", "description", "item_type"]:
        combined_monthly_data[col] = combined_monthly_data[f'{col}_cost'].combine_first(
            combined_monthly_data[f'{col}_price'])
        combined_monthly_data.drop(columns=[f'{col}_cost', f'{col}_price'], inplace=True)

    # add item master data
    # -- define columns of interest
    columns_to_add = [
        "level_1_category", "level_2_category", "level_3_category",
        "level_4_category", "level_5_category", "level_6_category",
        "vsi_mfr", "vsi_item_category"
    ]

    # -- create a view of `items` keyed by skusk:
    items_sub = items.set_index("sku")[columns_to_add]

    # -- join on the sku
    combined_monthly_data = combined_monthly_data.join(items_sub, on="sku", how="left")

    # calculate margin columns
    combined_monthly_data['inventory_avg_margin'] = combined_monthly_data['inventory_avg_unit_price'] - combined_monthly_data[
        'highest_recent_unit_cost']
    combined_monthly_data['inventory_avg_margin_pct'] = combined_monthly_data['inventory_avg_margin'] / combined_monthly_data[
        'inventory_avg_unit_price'] * 100

    combined_monthly_data['commission_avg_margin'] = combined_monthly_data['commission_avg_unit_price'] - combined_monthly_data[
        'highest_recent_unit_cost']
    combined_monthly_data['commission_avg_margin_pct'] = combined_monthly_data['commission_avg_margin'] / combined_monthly_data[
        'commission_avg_unit_price'] * 100

    # some months don't have sales, resulting in a division by 0
    combined_monthly_data['inventory_avg_margin_pct'] = combined_monthly_data['inventory_avg_margin_pct'].replace(-float('inf'), 0.0)
    combined_monthly_data['commission_avg_margin_pct'] = combined_monthly_data['commission_avg_margin_pct'].replace(-float('inf'), 0.0)

    new_column_order = [
        'item_name', 'description', 'item_type', 'manufacturer', 'level_1_category', 'level_2_category',
        'level_3_category', 'level_4_category', 'level_5_category', 'level_6_category',
        'inventory_avg_unit_price', 'monthly_inventory_qty', 'monthly_inventory_sales',
        'commission_avg_unit_price', 'monthly_commission_qty', 'monthly_commission_sales',
        'avg_unit_cost', 'monthly_quantity_purchased', 'monthly_spend', 'avg_quoted_cost', 'highest_recent_unit_cost',
        'inventory_avg_margin', 'inventory_avg_margin_pct',
        'commission_avg_margin', 'commission_avg_margin_pct',
    ]

    return combined_monthly_data[new_column_order]


# LOAD DATA
# attach to the data lake
print("Attaching to data lake...")
config = hf.load_config("config/datalake_config.json", flush_cache=True)
service_client = adl.get_azure_service_client(config["blob_url"])
file_system_client = adl.get_azure_file_system_client(service_client, "consolidated")


print("Retrieving data lake data...")
# get data lake data
trans_type = "CustInvc"
po_lines = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite", "transaction/PurchOrdItemLineItems_enhanced.parquet")
line_items = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite", f"transaction/{trans_type}ItemLineItems_enhanced.parquet")
items = adl.get_parquet_file_from_data_lake(file_system_client, "enhanced/netsuite", "item_enhanced.parquet")
print("Data retrieved.")

print("Cleaning, combining, and enhancing data...")
# CLEAN DATA
# drop all line items that don't have a highest_recent_cost
line_items = line_items[~line_items.highest_recent_cost.isna()]

# drop all po_lines before the earliest date in line_items
inv_state_date = line_items["created_date"].min()
po_lines = po_lines[po_lines["created_date"] >= inv_state_date]

# ENHANCE DATA
# combine and enhance invoice/po data
combined_monthly_data = combine_and_enhance_line_item_data(po_lines, line_items, items)
print("Data combined and enhanced.")

print("Saving enhanced data in data lake...")
# SAVE TO DATA LAKE
start_month = combined_monthly_data.index.get_level_values('month').min().strftime('%Y-%m')
end_month = combined_monthly_data.index.get_level_values('month').max().strftime('%Y-%m')
filename = f"monthly_{trans_type}_margin_analysis_dataset_{start_month}_{end_month}.parquet"
adl.save_df_as_parquet_in_data_lake(combined_monthly_data, file_system_client, "presentation/margin_analysis", filename, preserve_index=True)
print("Dataset saved to data lake.")