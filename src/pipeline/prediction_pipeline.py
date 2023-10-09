import os, sys

from src.config.spark_manager import spark_session
from src.constants.prediction_pipeline import *
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

            for user in user_list:
                user_path = os.path.join(INPUT_DIR, user)

                temp_df = self.read_data(user_path)

                cols = temp_df.column

                no_col_list = [col for col in REQUIRED_COLUMNS if col not in cols]

                if len(no_col_list) > 0:
                    invalid_files.append(user)
                else:
                    valid_files.append(user)
            
                

        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)
    def initiate_batch_prediction(self):
        try:
            logging.info("Entered initiate_batch_prediction method")
            
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)