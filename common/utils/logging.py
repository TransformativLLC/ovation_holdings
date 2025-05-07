"""
This module provides shared logging functions.
"""

### IMPORTS ###
import logging


# delegating to each module to simplify __init__.py
__all__ = [
    "create_logger"
]


### FUNCTIONS ###
def create_logger(logger_name: str, log_level: int = logging.INFO, base_dir: str = "./logs") -> logging.Logger:
    """
    Creates a logger object for logging messages to a file.

    Args:
        logger_name (str): The name of the logger.
        log_level (int): The logging level (default is logging.INFO).
        base_dir (str): The base directory where the log file will be saved.

    Returns:
        logging.Logger: A logger object for logging messages.
    """

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    if not logger.handlers:
        log_file = f"{base_dir}/{logger_name}.log"

        logger.propagate = False

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger
