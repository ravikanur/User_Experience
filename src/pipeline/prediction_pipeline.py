import os, sys

from src.config.spark_manager import spark_session
from src.components.data_validation import DataValidation

from src.logger import logging
from src.exception import UserException

class PredictionPipeline:
    def read_data(self):
        try:
            logging.info("Entered read_data method")
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)
    def get_valid_invalid_files(self):
        try:
            logging.info("Entered get_valid_invalid_files method")
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)
    def initiate_batch_prediction(self):
        try:
            logging.info("Entered initiate_batch_prediction method")
            
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)