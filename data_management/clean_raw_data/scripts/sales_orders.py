# IMPORTS
import pandas as pd
import numpy as np
import re


# FUNCTIONS
def set_subsidiary_by_location(df: pd.DataFrame, location_map: dict,
                               null_value: str = "null", replacement_value = "Not Specified") -> pd.DataFrame:
    """
    Sets the subsidiary name based on the location of the company.

    Args:
        df (pd.DataFrame): The DataFrame containing company data.
        location_map (dict): The dictionary that holds the mapping of locations to subsidiaries.
        null_value (str): The value to use for null locations. Defaults to "null".
        replacement_value: The value to use for locations not found in the location_map. Defaults to "Not Specified".

    Returns:
        pd.DataFrame: The DataFrame with the 'Subsidiary' column updated based on the location.
    """

    # Replace subsidiary_name only if location is not "null"
    df["subsidiary_name"] = np.where(
        df["location"] == null_value,
        df["subsidiary_name"],
        df["location"].map(location_map)
    )

    return df


def compare_transactions_and_line_items(transactions: pd.DataFrame, line_items: pd.DataFrame) -> pd.DataFrame:
    """
    Compare the net amount at the transaction level to the sum of total amounts of their associated line items.

    Args:
        transactions (pd.DataFrame): DataFrame containing transactions with 'tranid' and 'net_amount'.
        line_items (pd.DataFrame): DataFrame containing line items with 'tranid' and 'total_amount'.

    Returns:
        pd.DataFrame: A DataFrame showing the transaction ID (tranid), net amount from transaction level,
                      and the sum of total amount from line items, along with the difference.
    """
    # Group sales_orders to get net amount per tranid
    transaction_summary = transactions.groupby("tranid", as_index=False).agg(
        transaction_net_amount=('net_amount', 'sum')
    )

    # Group line items to get the total_amount per tranid
    line_items_summary = line_items.groupby("tranid", as_index=False).agg(
        line_items_total_amount=('total_amount', 'sum')
    )

    # Merge the two summaries on 'tranid'
    comparison_df = pd.merge(
        transaction_summary, line_items_summary, on="tranid", how="inner"
    )

    # Calculate the difference between sales order net amount and line items total amount
    comparison_df["difference"] = (
        comparison_df["transaction_net_amount"] - comparison_df["line_items_total_amount"]
    )

    # Sort the results by the difference to highlight discrepancies
    comparison_df = comparison_df.sort_values(by="difference", ascending=False)

    return comparison_df


def clean_illegal_excel_chars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove illegal characters from all string columns in the DataFrame.
    Excel does not allow certain control characters in cells.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A cleaned DataFrame without illegal Excel characters.
    """
    # Define a regular expression to match illegal control characters
    illegal_chars = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]')

    # Clean only string columns
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].map(lambda x: illegal_chars.sub("", x) if isinstance(x, str) else x)

    return df