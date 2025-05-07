"""
This module provides shared functions for managing configuration.
"""

### IMPORTS ###
import json


### FUNCTIONS ###
def load_config(file_path: str, flush_cache: bool = False) -> dict:
    """
    Lazily loads a configuration from a JSON file and caches it.

    Args:
        file_path (str): The path to the JSON configuration file.
        flush_cache (bool): Whether to flush the cache and reload the configuration. Defaults to False.

    Returns:
        dict: The configuration dictionary loaded from the JSON file.
    """
    if not hasattr(load_config, "_cache"):
        load_config._cache = {}

    if file_path not in load_config._cache or flush_cache:
        with open(file_path, 'r') as file:
            load_config._cache[file_path] = json.load(file)

    return load_config._cache[file_path]

