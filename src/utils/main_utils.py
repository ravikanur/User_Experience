import os, sys
import yaml

from pyspark.sql import dataframe
from src.exception import UserException
from src.logger import logging

def read_yaml_file(path: str) -> dict:
    try:
        logging.info("Entering read_yaml_file function")
        with open(path, 'rb') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.error(e)
        raise UserException(e,sys) from e

def write_yaml_file(path: str, filename: dict):
    try:
        logging.info("Entering write_yaml_file function")
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'w') as file:
            yaml.dump(filename, file)
    except Exception as e:
        logging.error(e)
        raise UserException(e, sys) from e

def rearrange_dataframe_columns(ref_df: dataframe, curr_df: dataframe) -> dataframe:
    try:
        logging.info("Entered rearrange_dataframe_columns method")
        ref_cols = ref_df.columns

        curr_cols = curr_df.columns

        if len(ref_cols) != len(curr_cols):
            raise Exception("Length of curr_df is not same as lenght of ref_df")

        curr_df1 = curr_df.select(ref_cols)
        return curr_df1
    except Exception as e:
        logging.error(e)
        raise UserException(e, sys) from e