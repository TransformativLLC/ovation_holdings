# IMPORTS
# typing libraries
from typing import List, Dict, Tuple

# file management libraries
import os
import io
import json

# Azure Data Lake libraries
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from azure.identity import AzureCliCredential
from azure.core.exceptions import ResourceNotFoundError

# multithreading libraries
import concurrent.futures as cf

# data manipulation libraries
import pandas as pd

# progress bar library
from tqdm import tqdm


# LOAD ENVIRONMENT VARIABLES
from dotenv import load_dotenv
from pathlib import Path

# load the .env shipped with this package on import
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=False)


# delegating to each module to simplify __init__.py
__all__ = [
    "get_azure_service_client",
    "get_azure_file_system_client",
    "get_parquet_file_from_data_lake",
    "save_df_as_parquet_in_data_lake",
    "convert_json_to_parquet",
    "get_transactions_and_line_items",
]


# FUNCTIONS
def get_azure_service_client(storage_url_env_var: str) -> DataLakeServiceClient:
    """
    Get an Azure Data Lake Service Client using Azure CLI credentials.

    Args:
        storage_url_env_var (str): name of environment variable that holds URL of the Azure Storage Account

    Returns:
        service_client: DataLakeServiceClient
    """

    # load environment variables
    AZURE_STORAGE_BLOB_URL = os.getenv(storage_url_env_var)

    # Authenticate using Azure CLI credentials
    credential = AzureCliCredential()

    # Create the service client
    service_client = DataLakeServiceClient(account_url=AZURE_STORAGE_BLOB_URL, credential=credential)

    return service_client


def get_azure_file_system_client(service_client: DataLakeServiceClient, file_system_name: str) -> FileSystemClient:
    """
    Get an Azure Data Lake File System Client.

    Args:
        service_client: DataLakeServiceClient
        file_system_name: str

    Returns:
        file_system_client: DataLakeFileSystemClient
    """

    # Get the File System Client
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)

    return file_system_client


def read_file_from_data_lake(file_system_client: FileSystemClient, file_path: str) -> pd.DataFrame:
    """Read a single file from Azure Data Lake Gen2 into a DataFrame.

    Args:
        file_system_client (FileSystemClient): The client to access the file system.
        file_path (str): The path of the file to read.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the file.
    """
    try:
        file_client = file_system_client.get_file_client(file_path)
        download = file_client.download_file()
        record = json.loads(download.readall().decode('utf-8'))

        # top level files have only one record
        # line item files have a list of records
        if isinstance(record, list):
            df = pd.DataFrame(record)  # line df
        else:
            df = pd.DataFrame([record])  # top level record

    except Exception as e:
        # Handle errors gracefully and return an empty DataFrame
        print(f"Error reading file {file_path}: {e}")
        df = pd.DataFrame()

    return df


def read_files_in_batches_from_data_lake(file_system_client: FileSystemClient, source_dir: str, file_names: List[str],
                                         batch_size: int = 1000, max_workers: int = 15) -> pd.DataFrame:
    """Read multiple files from Azure Data Lake Gen2 into a single DataFrame using multithreading with batching.

    Args:
        file_system_client (FileSystemClient): The client to access the file system.
        source_dir (str): The directory to read files from.
        file_names (List[str]): The list of file paths to read.
        batch_size (int, optional): Number of files to read in each batch. Defaults to 1000.
        max_workers (int, optional): Maximum number of worker threads. Defaults to 15.

    Returns:
        pd.DataFrame: A DataFrame containing the data from all files.
    """
    dataframes = []

    with tqdm(total=len(file_names), desc=f"Processing {source_dir} Files") as progress_bar:
        for i in range(0, len(file_names), batch_size):
            batch_files = file_names[i:i + batch_size]

            with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit tasks to the executor
                futures = [executor.submit(read_file_from_data_lake, file_system_client, f"{source_dir}/{file}") for
                           file in batch_files]

                # Process results as they complete
                for future in cf.as_completed(futures):
                    df = future.result()
                    if not df.empty:
                        dataframes.append(df)
                    progress_bar.update(1)

    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
    else:
        print("No dataframes to concatenate")
        final_df = pd.DataFrame()

    return final_df


def get_paths_by_directory(
        file_system_client: FileSystemClient,
        start_directory: str = "/",
        extension: str = ".json",
        max_workers: int = 15
) -> Dict[str, List[str]]:
    """
    Filters paths in a file system client based on a given file extension and organizes
    them into a dictionary with directories as keys and files as values.

    Args:
        file_system_client (FileSystemClient): The file system client.
        start_directory (str): The directory to start searching from (default is '/').
        extension (str): The file extension to filter on (default is '.json').
        max_workers (int): The maximum number of worker threads to use (default is 10).

    Returns:
        Dict[str, List[str]]: A dictionary where keys are directories/subdirectories
                              and values are lists of files with the specified extension.
    """
    paths = file_system_client.get_paths(path=start_directory)

    def is_target_file(path_item):
        if path_item.name.endswith(extension):
            # Extract directory and filename
            *dirs, filename = path_item.name.split("/")
            directory = "/".join(dirs)
            return directory, filename
        return None

    # Use ThreadPoolExecutor for parallel filtering
    with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
        filtered = list(executor.map(is_target_file, paths))

    # Organize into a dictionary
    directory_to_files = {}
    for result in filtered:
        if result:
            directory, filename = result
            if directory not in directory_to_files:
                directory_to_files[directory] = []
            directory_to_files[directory].append(filename)

    return directory_to_files


def save_df_as_parquet_in_data_lake(df: pd.DataFrame, file_system_client, azure_directory_path: str,
                                    file_name: str, preserve_index: bool = False) -> None:
    """
    Save a DataFrame as a Parquet file in the Data Lake.

    Args:
        df (pd.DataFrame): DataFrame to save
        file_system_client: DataLakeFileSystemClient
        azure_directory_path (str): path to the directory in the Data Lake
        file_name (str): name of the Parquet file
        preserve_index (bool): whether to preserve the index of the DataFrame

    Returns:
        None
    """

    # Create a BytesIO buffer
    parquet_buffer = io.BytesIO()

    # Write DataFrame to the buffer in Parquet format
    df.to_parquet(parquet_buffer, engine='pyarrow', index=preserve_index)

    # Reset the buffer's position to the beginning
    parquet_buffer.seek(0)

    # Define the path where you want to save the Parquet file in data lake
    parquet_target_path = os.path.join(azure_directory_path, file_name)

    # Get a File Client for the Parquet file
    parquet_file_client = file_system_client.get_file_client(parquet_target_path)

    # Upload the buffer's content to data lake
    parquet_file_client.upload_data(parquet_buffer.read(), overwrite=True)


def get_parquet_file_from_data_lake(file_system_client, azure_directory_path: str, file_name: str) -> pd.DataFrame:
    """
    Get a Parquet file from the Data Lake.

    Args:
        file_system_client: DataLakeFileSystemClient
        azure_directory_path (str): path to the directory in the Data Lake
        file_name (str): name of the Parquet file

    Returns:
        df (pd.DataFrame): DataFrame containing the Parquet file's data
    """

    # Define the path to the Parquet file in data lake
    parquet_target_path = os.path.join(azure_directory_path, file_name)

    # Get a File Client for the Parquet file
    parquet_file_client = file_system_client.get_file_client(parquet_target_path)

    # Download the Parquet file into a BytesIO buffer
    download = parquet_file_client.download_file()

    # Create an in-memory bytes buffer
    parquet_bytes = download.readall()
    parquet_buffer = io.BytesIO(parquet_bytes)

    # Read the Parquet file from the buffer into a DataFrame
    return pd.read_parquet(parquet_buffer, engine='pyarrow')


def convert_json_to_parquet(service_client: DataLakeServiceClient,
                            source_container_name: str,
                            source_directory: str,
                            target_container_name: str = "consolidated",
                            target_directory: str = "raw") -> None:
    """
    Converts JSON files in a single directory in the source container to Parquet files in target directory in the target container.

    Args:
        service_client: DataLakeServiceClient
        source_container_name (str): name of the source container
        source_directory (str): name of the source directory
        target_container_name (str): name of the target container (default is "consolidated")
        target_directory (str): name of the target directory (default is "raw")

    Returns:
        None

    Side Effects:
        - Creates/Saves Parquet files to the target directory in the target container.
    """

    file_system_client = get_azure_file_system_client(service_client, source_container_name)
    file_paths = get_paths_by_directory(file_system_client, start_directory=source_directory)
    df = read_files_in_batches_from_data_lake(file_system_client, source_directory, file_paths[source_directory])
    file_system_client = get_azure_file_system_client(service_client, target_container_name)
    save_df_as_parquet_in_data_lake(df, file_system_client, f"{target_directory}/{source_container_name}",
                                    f"{source_directory}_{target_directory}.parquet")


def get_transactions_and_line_items(file_system_client: FileSystemClient,
                                    trans_type: str, data_state: str = "raw") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retrieves transactions and associated line df from Azure Data Lake based on the transaction type.

    Args:
        file_system_client (FileSystemClient): The client instance for interacting with the Azure Data Lake file system.
        trans_type (str): The type of transaction to fetch. Must be one of ["Estimate", "SalesOrd", "PurchOrd", "CustInvc"].
        data_state (str): The state of the data determines where to get it. Defaults to "raw".

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames:
            - The first DataFrame contains the transactions.
            - The second DataFrame contains the associated line df.

    Raises:
        ValueError: If `trans_type` is not one of the supported transaction types.
        ResourceNotFoundError: If enhanced data is requested but not found, falls back to cleaned data.
    """

    if trans_type not in ["Estimate", "SalesOrd", "PurchOrd", "CustInvc"]:
        raise ValueError(f"Invalid value for trans_type: {trans_type}")

    data_state = data_state.lower()

    # transactions are cleaned but usually not enhanced, so, need to fall back to cleaned if not available
    if data_state == "enhanced":
        try:
            transactions = get_parquet_file_from_data_lake(file_system_client, f"{data_state}/netsuite",
                                                           f"transaction/{trans_type}_{data_state}.parquet")
        except ResourceNotFoundError:
            data_state = "cleaned"
            transactions = get_parquet_file_from_data_lake(file_system_client, f"{data_state}/netsuite",
                                                           f"transaction/{trans_type}_{data_state}.parquet")
    else:
        transactions = get_parquet_file_from_data_lake(file_system_client, f"{data_state}/netsuite",
                                                       f"transaction/{trans_type}_{data_state}.parquet")

    line_items = get_parquet_file_from_data_lake(file_system_client, f"{data_state}/netsuite",
                                                 f"transaction/{trans_type}ItemLineItems_{data_state}.parquet")

    return transactions, line_items
