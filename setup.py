# setup.py
from setuptools import setup, find_packages

setup(
    name="Ovation_Holdings",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        # <package>.<sub-package>: [<file-patterns>]
        "common.config": ["*.json"],
    },
)
