import argparse
import os, sys

from src.pipeline.training_pipeline import TrainingPipeline
from src.pipeline.prediction_pipeline import PredictionPipeline

from src.logger import logging
from src.exception import UserException

def initiate_training():
    try:
        logging.info("Entered initiate_training method")
    except Exception as e:
        logging.error(UserException(e, sys))
        raise UserException(e, sys)
        

def initiate_prediction(path: str):
    try:
        logging.info("Entered initiate_prediction method")
        pred_pipeline = PredictionPipeline()

        pred_pipeline.initiate_batch_prediction(path)
    except Exception as e:
        logging.error(UserException(e, sys))
        raise UserException(e, sys)

def main(action: int, path: str):
    try:
        logging.info("Entered main method")
        if action == 0:
            initiate_prediction(path)
        elif action == 1:
            initiate_training(path)
        else:
            raise UserException("Wrong input provided for action", sys)
    except Exception as e:
        logging.error(UserException(e, sys))
        raise UserException(e, sys)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #parser.add_argument("--a", type=str, help="Type of action to be taken. 0 for prediction, 1 for training")
    parser.add_argument("--p", type=str, help="Path of the input data")
    args = parser.parse_args()
    initiate_prediction(args.p)
