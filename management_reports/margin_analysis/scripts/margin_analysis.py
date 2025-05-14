# IMPORTS

# Data typing libraries
from typing import Tuple

# Data analysis libraries
import pandas as pd

# Data visualization libraries
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


# FUNCTIONS
def calculate_total_booked_sales(df: pd.DataFrame, grouping_columns: list, time_period: str = "month") -> pd.DataFrame:
    """
    Calculate total booked sales, gross profit, quantity sold, and order count for the specified grouping columns.
    Allows grouping by a specified time period when "created_date" is included.

    Args:
        df (pd.DataFrame): The input DataFrame containing line item data.
        grouping_columns (list): A list of columns to group by (e.g., "sku", "customer_name", etc.).
        time_period (str, optional): Time period granularity for "created_date". One of "year", "month", or "week".
                                     Defaults to "month".

    Returns:
        pd.DataFrame: A DataFrame with calculated metrics for each group.
    """
    # Ensure created_date is in datetime format if used in grouping
    if "created_date" in df.columns:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")

    # Handle time period grouping if "created_date" is included
    if "created_date" in grouping_columns:
        if time_period == "year":
            df["time_period"] = df["created_date"].dt.to_period("Y")
        elif time_period == "month":
            df["time_period"] = df["created_date"].dt.to_period("M")
        elif time_period == "week":
            df["time_period"] = df["created_date"].dt.to_period("W")
        else:
            raise ValueError("Invalid time_period. Must be one of 'year', 'month', or 'week'.")

        # Replace "created_date" in grouping_columns with the new time period column
        grouping_columns = [
            "time_period" if col == "created_date" else col for col in grouping_columns
        ]

    # Perform group-by and calculate metrics
    grouped_df = df.groupby(grouping_columns).agg(
        avg_cost=('est_extended_cost', 'mean'),
        avg_unit_price=('unit_price', 'mean'),
        total_booked_sales=('total_amount', 'sum'),
        total_gross_profit=('gross_profit', 'sum'),
        order_count=('sku', 'count'),
        qty_sold=('quantity', 'sum')
    ).reset_index()

    # Calculate additional metrics
    grouped_df["qty_sold"] = -1 * grouped_df["qty_sold"]
    grouped_df["avg_gross_profit_per_unit"] = grouped_df["total_gross_profit"] / grouped_df["qty_sold"]
    grouped_df["avg_gross_margin_pct"] = grouped_df["total_gross_profit"] / grouped_df["total_booked_sales"]

    # fill nan values in avg_gross_margin_pct with 0
    grouped_df["avg_gross_margin_pct"].fillna(0, inplace=True)

    # fill -inf and inf values in avg_gross_margin_pct with 0
    grouped_df["avg_gross_margin_pct"] = grouped_df["avg_gross_margin_pct"].replace([float('inf'), float('-inf')], 0)

    # Sort by total booked sales in descending order
    num_sort_items = min(len(grouping_columns), 3)  # Sort by up to 3 columns
    grouped_df = grouped_df.sort_values(grouping_columns[:num_sort_items], ascending=True)

    return grouped_df


def filter_top_skus(sales_df: pd.DataFrame, top_skus_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the sales DataFrame to include only rows corresponding to the top SKUs for each subsidiary.

    Args:
        sales_df (pd.DataFrame): The DataFrame containing monthly booked sales by SKU and subsidiary.
        top_skus_df (pd.DataFrame): The DataFrame containing the top SKUs for each subsidiary.

    Returns:
        pd.DataFrame: A filtered DataFrame with sales data for only the top SKUs in each subsidiary.
    """
    # Extract the unique (subsidiary_name, location, sku) pairs from the top_skus_df
    top_sku_pairs = top_skus_df[["subsidiary_name", "location", "sku"]].drop_duplicates()

    # Filter sales_df to include only rows where (subsidiary_name, sku) pairs match top_sku_pairs
    filtered_sales_df = sales_df.merge(top_sku_pairs, on=["subsidiary_name", "location", "sku"], how="inner")

    return filtered_sales_df


def fill_missing_months(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in missing months for each SKU and carry forward the avg_gross_margin_percent.

    Args:
        df (pd.DataFrame): The input DataFrame containing monthly gross margin data.

    Returns:
        pd.DataFrame: A DataFrame with missing months filled in and avg_gross_margin_percent carried forward.
    """

    # Ensure created_date is a datetime type
    if df["created_date"].dtype == "period[M]":  # Check if created_date is a period type
        df["created_date"] = df["created_date"].dt.to_timestamp()  # Convert Period to Timestamp

    # Set up a complete date range for the 36 months
    full_date_range = pd.date_range(start=df["created_date"].min(), end=df["created_date"].max(), freq='MS')

    # Create an empty list to hold the filled data for each SKU and subsidiary combination
    filled_data = []

    # Iterate over each SKU and subsidiary combination
    for (subsidiary, location, sku), group in df.groupby(["subsidiary_name", "location", "sku"]):
        # Set created_date as the index for reindexing
        group = group.set_index("created_date")

        # Reindex to the full date range
        group = group.reindex(full_date_range)

        # Set subsidiary_name, location, and sku columns after reindexing
        group["subsidiary_name"] = subsidiary
        group["location"] = location
        group["sku"] = sku

        # Forward fill missing avg_gross_margin_percent values
        group["avg_gross_margin_pct"] = group["avg_gross_margin_pct"].ffill()

        # Fill other missing columns with appropriate values
        group["total_booked_sales"] = group["total_booked_sales"].fillna(0)  # Fill missing total_amount with 0
        group["total_gross_profit"] = group["total_gross_profit"].fillna(0)  # Fill missing total_gross_profit with 0
        group["order_count"] = group["order_count"].fillna(0)  # Fill missing order_count with 0
        group["qty_sold"] = group["qty_sold"].fillna(0)  # Fill missing quantity with 0
        group["avg_gross_profit_per_unit"] = group["avg_gross_profit_per_unit"].fillna(0)

        # Reset the index back to created_date
        group = group.reset_index().rename(columns={"index": "created_date"})

        # Append the filled group to the list
        filled_data.append(group)

    # Concatenate all the filled groups back together
    filled_df = pd.concat(filled_data, ignore_index=True)

    return filled_df


def fill_missing_string_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing string values for categorical columns using forward fill.

    Args:
        df (pd.DataFrame): The input DataFrame with missing string values after reindexing.

    Returns:
        pd.DataFrame: A DataFrame with missing string values filled in.
    """
    # Identify columns to fill (all object-type columns)
    string_columns = df.select_dtypes(include=['object']).columns

    # Use forward/back fill to fill in missing values for string columns
    df[string_columns] = df[string_columns].ffill().bfill()

    return df


def calculate_ttm_avg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate trailing twelve months (TTM) average gross margin percentage for each SKU.

    Args:
        df (pd.DataFrame): The DataFrame containing monthly gross margin data.

    Returns:
        pd.DataFrame: The original DataFrame with an additional 'ttm_avg_gross_margin_percent' column.
    """
    # Sort the DataFrame
    df = df.sort_values(by=["subsidiary_name", "location", "sku", "created_date"])

    # Calculate TTM average using rolling window
    df["ttm_avg_gross_margin_percent"] = (
        df.groupby(["subsidiary_name", "location", "sku"])["avg_gross_margin_pct"]
        .rolling(window=12, min_periods=1)
        .mean()
        .reset_index(level=[0, 1, 2], drop=True)
    )

    return df


def plot_all_sku_margin_trends(df: pd.DataFrame, sku_group: str, figsize: Tuple[int, int] = (15, 10)) -> None:
    """
    Plot the trailing twelve months (TTM) average gross margin percentage for all SKUs.

    Args:
        df (pd.DataFrame): The DataFrame containing the TTM average gross margin data.
        sku_group (str): Name of the group of skus plotted. Used in chart title.
        figsize (Tuple[int, int], optional): The figure size for the plot. Defaults to (15, 10).

    Returns:
        None
    """

    plt.figure(figsize=figsize)

    # Iterate over each group by "sku"
    for sku, group in df.groupby("sku"):
        # Retrieve the item_name for the current group
        item_name = group["item_name"].iloc[0]  # Assuming all rows in the group have the same item_name

        # Create a combined label for the legend
        combined_label = f"{sku} - {item_name}"

        # Plot the data with the combined label
        plt.plot(group["created_date"], group["ttm_margin_change_pct"], label=combined_label)

    # Add legend and labels to the plot
    plt.legend(title="SKU - Item Name", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.title(f"{sku_group} TTM Margin Change Percentage")
    plt.xlabel("Date")
    plt.ylabel("TTM Margin Change Percentage")
    plt.show()


def plot_individual_sku_margin_trends(df: pd.DataFrame, figsize: Tuple[int, int] = (10, 6)) -> None:
    """
    Plot the trailing twelve months (TTM) average gross margin percentage for each SKU.

    Args:
        df (pd.DataFrame): The DataFrame containing the TTM average gross margin data.

    Returns:
        None
    """

    for sku, group in df.groupby("sku"):
        # Sort group by created_date to ensure proper order
        group = group.sort_values(by="created_date")

        # Retrieve the item_name for the current group
        item_name = group["item_name"].iloc[0]  # Assuming item_name is consistent within each group

        # Create a line graph for each SKU
        plt.figure(figsize=figsize)
        plt.plot(group["created_date"], group["ttm_avg_gross_margin_percent"], marker='o', linestyle='-', color='b')

        # Set titles and labels
        plt.title(f'{sku} - {display_name}')
        plt.xlabel('Date')
        plt.ylabel('TTM Gross Margin Percentage (%)')
        plt.xticks(rotation=45)  # Rotate x-axis labels for better readability

        # Show the plot
        plt.tight_layout()  # Adjust layout for better fitting
        plt.show()


def create_margin_trend_pdf(df: pd.DataFrame, file_name: str, pdf_dir: str = "pdfs") -> None:
    """
    # Create a PDF file

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be plotted.
        file_name (str): The name of the PDF file to be created.
        pdf_dir (str): The directory where the PDF file will be saved. Default is "pdfs"

    Returns:
        None
    """

    # strip filename of extension
    file_name = file_name.split(".")[0]

    with PdfPages(f'{pdf_dir}/{file_name}.pdf') as pdf:
        # Iterate over each SKU group
        for sku, group in df.groupby("sku"):
            # Sort group by created_date to ensure proper order
            group = group.sort_values(by="created_date")

            # Retrieve the item_name for the current group
            item_name = group["item_name"].iloc[0]  # Assuming item_name is consistent within each group

            # Create a line graph for each SKU
            plt.figure(figsize=(10, 6))
            plt.plot(group["created_date"], group["ttm_avg_gross_margin_percent"], marker='o', linestyle='-', color='b')

            # Set titles and labels
            plt.title(f'{sku} - {item_name}')
            plt.xlabel('Date')
            plt.ylabel('TTM Gross Margin Percentage (%)')
            plt.xticks(rotation=45)  # Rotate x-axis labels for better readability

            # Adjust layout for better fitting
            plt.tight_layout()

            # Save the current figure to the PDF file
            pdf.savefig()  # Saves the current figure into the PDF
            plt.close()  # Close the current figure to free memory


