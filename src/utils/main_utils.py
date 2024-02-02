import os, sys
import yaml

from pyspark.sql import dataframe
from pyspark.sql.functions import monotonically_increasing_id
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

def join_dataframe(df1: dataframe, df2: dataframe, df1_cols: list = None, df2_cols: list = None,
                    df1_key_col: str = None, df2_key_col: str = None):
    try:
        logging.info("Entered join_dataframe method")
        if df1_cols != None:
            df1 = df1.select(*df1_cols)

        if df2_cols != None:
            df2 = df2.select(*df2_cols)

        if df1_key_col != None and df2_key_col != None:
            df1 = df1.join(df2, df1[df1_key_col] == df2[df2_key_col], 'inner')
        elif df1_key_col == None and df2_key_col == None:
            df1 = df1.withColumn('id', monotonically_increasing_id())

            df2 = df2.withColumn('id', monotonically_increasing_id())

            df1 = df1.join(df2, df1.id == df2.id, 'inner')
        else:
            raise UserException("Please provide both df1_key_col and df2_key_col else don't provide both", sys)

        return df1
    except Exception as e:
        logging.error(e)
        raise UserException(e, sys)