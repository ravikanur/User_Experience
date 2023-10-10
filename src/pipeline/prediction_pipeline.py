import os, sys

from pyspark.sql import dataframe
from pyspark.sql.functions import lit
from src.config.spark_manager import spark_session
from src.constants.prediction_pipeline import *
from src.constants.training_pipeline import *
from src.components.data_validation import DataValidation

from src.logger import logging
from src.exception import UserException

class PredictionPipeline:
    def read_data(self, path:str):
        try:
            logging.info("Entered read_data method")
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)
    def get_valid_invalid_files(self):
        try:
            logging.info("Entered get_valid_invalid_files method")
            user_list = os.listdir(INPUT_DIR)

            valid_files, invalid_files = [], []

            for i, user in enumerate(user_list):
                user_path = os.path.join(INPUT_DIR, user)

                user_name = user.split(sep='.')[0]

                temp_df: dataframe = spark_session.read.csv(path, inferSchema=True, header=True)

                cols = temp_df.columns

                no_col_list = [col for col in REQUIRED_COLUMNS if col not in cols]

                if len(no_col_list) > 0:
                    invalid_files.append(user)
                else:
                    valid_files.append(user)
                    temp_df = temp_df.withColumn(USER_COLUMN_NAME, lit(user_name))
                    if i == 0:
                        final_df = temp_df
                    else:
                        final_df = final_df.union(temp_df)
            return valid_files, invalid_files, final_df
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)
    def initiate_batch_prediction(self):
        try:
            logging.info("Entered initiate_batch_prediction method")
            
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)