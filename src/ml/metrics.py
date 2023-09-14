import sys

from pyspark.sql import DataFrame
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from src.logger import logging
from src.exception import UserException

def calculate_classification_metric(model, test_data: DataFrame, labelCol: str, metric_name:str = None):
    try:
        logging.info("Entered calculate_classification_metric method")
        pred = model.transform(test_data)

        ev = BinaryClassificationEvaluator(labelCol= labelCol)

        if metric_name != None:
            ev.setMetricName(metric_name)
        acc = ev.evaluate(pred)

        logging.info(f"accuracy of the model is {acc} and the evaluation metric used is {ev.getMetricName()}")

        return acc, pred
    except Exception as e:
        logging.error(e)
        raise UserException(e, sys)