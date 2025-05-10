"""
This module provides shared functions for managing configuration.
"""

### IMPORTS ###
import json


# delegating to each module to simplify __init__.py
__all__ = [
    "load_config",
]


### FUNCTIONS ###
from importlib import resources
from pathlib import Path
import json
import types
from typing import Union

def load_config(source: Union[str, types.ModuleType], filename: str | None = None) -> dict:
    """
    Load a JSON config either from disk (when source is a path string)
    or from a Python package (when source is a module and filename is set).

    Args:
        source:
          - A filesystem path (str or Path) _or_
          - a package/module object.
        filename:
          - Name of the JSON file when loading from a package.
          - Ignored when source is a path.

    Returns:
        The parsed JSON as a dict.
    """
    # file-system path
    if isinstance(source, (str, Path)):
        p = Path(source)
        with p.open("r") as f:
            return json.load(f)

    # package-resource
    if filename is None:
        raise ValueError("When loading from a package, `filename` must be provided")
    with resources.path(source, filename) as p:
        with p.open("r") as f:
            return json.load(f)
