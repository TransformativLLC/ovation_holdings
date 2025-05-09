import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def describe_dataset(df: pd.DataFrame) -> str:
    """Prints descriptive information about the provided DataFrame.

    This function analyzes and prints key information about the dataset including:
    - Date range of the data
    - Number of unique inventory df
    - List of columns with their data types

    Args:
        df: A pandas DataFrame containing inventory data with a MultiIndex that includes
            a 'month' level and columns including 'item_name'.

    Returns:
        str: A string describing the dataset.
    """
    start_month = df.index.get_level_values('month').min().strftime('%Y-%m')
    end_month = df.index.get_level_values('month').max().strftime('%Y-%m')

    output_str = ""
    output_str += f"**Data Range**: {start_month} to {end_month}<br>"
    output_str += f"**Number of Inventory Items**: {df.item_name.unique().shape[0]:,}<br>"
    output_str += "**Fields in Dataset**:<br>"
    for col in sorted(df.columns):
        dtype = df[col].dtype
        if dtype == 'object':
            type_name = 'text'
        elif np.issubdtype(dtype, np.number):
            type_name = 'number'
        else:
            type_name = 'other'
        output_str += f"&ensp;&ensp;&ensp;- {col} ({type_name})<br>"

    return output_str


def calculate_cumulative_sales(data, sales_column, result_column_name):
    """
    Calculate cumulative sales by item and merge it with related metadata.

    Args:
        data (pd.DataFrame): The input data frame containing sales data.
        sales_column (str): The column name for sales to aggregate.
        result_column_name (str): The name of the cumulative sales column in the result.

    Returns:
        pd.DataFrame: DataFrame with cumulative sales and related metadata merged.
    """
    # Group by SKU and calculate cumulative sales
    cumulative_sales = data.groupby(level='sku')[sales_column].sum().sort_values(ascending=False).rename(result_column_name)

    # Select metadata and merge
    metadata = data.groupby(level='sku').agg({
        'manufacturer': 'first',
        'item_name': 'first',
        'item_type': 'first',
        'level_1_category': 'first',
        'level_2_category': 'first',
        'level_3_category': 'first'
    })

    return pd.merge(cumulative_sales, metadata, left_index=True, right_index=True)


def plot_manufacturer_margins(df, margin_col, manufacturer, start_date='2022-01', end_date=pd.Timestamp.today().strftime('%Y-%m'),
                              display_plots=True, save_as_pdf=True, pdf_path="pdfs/inventory") -> None:

    # Reset the index to get manufacturer names
    manufacturer_data = df.reset_index()

    # Filter data for the specific manufacturer and date range
    data = manufacturer_data[
        (manufacturer_data['manufacturer'] == manufacturer) &
        (manufacturer_data['month'] >= start_date) &
        (manufacturer_data['month'] <= end_date)
        ]

    # Drop any NaN or infinite values
    data = data[~data[margin_col].isna() & ~np.isinf(data[margin_col])]

    plt.figure(figsize=(15, 10))

    if not data.empty:
        x = np.arange(len(data))
        y = data[margin_col].values

        # there may not be enough data for a plot or for trend calc to converge
        try:
            # Fit a polynomial of degree 1
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            trend_line_calculated = True
        except:
            trend_line_calculated = False

        # Plot actual values and trend line
        plt.plot(data['month'].astype(str), data[margin_col], marker='o', label=manufacturer, alpha=0.5)

        if trend_line_calculated:
            plt.plot(data['month'].astype(str), p(x), "r--", label='Trend')

        plt.title(f'Monthly Average Margin % for {manufacturer}')
        plt.xlabel('Month')
        plt.ylabel('Average Margin %')
        plt.ylim(bottom=0)
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()

        if save_as_pdf:
            filename = f"{pdf_path}/{manufacturer.replace(' ', '_')}_{margin_col}.pdf"
            plt.savefig(filename, bbox_inches='tight', format='pdf')

        if display_plots:
            plt.show()

        plt.close()