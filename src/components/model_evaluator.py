import os, sys, importlib

from pyspark.sql import DataFrame
from pyspark.ml.pipeline import PipelineModel

from src.config.spark_manager import spark_session
from src.constants.training_pipeline import *
from src.entity.config_entity import ModelEvaluatorConfig
from src.entity.artifact_entity import ModelEvaluatorArtifact, ModelTrainerArtifact, DataTransformationArtifact, ModelEvaluationResponse

from src.ml.metrics import calculate_classification_metric
from src.utils.main_utils import write_yaml_file

from src.logger import logging
from src.exception import UserException

class ModelEvaluator:
    def __init__(self, model_evaluator_config: ModelEvaluatorConfig, 
                model_trainer_artifact: ModelTrainerArtifact, 
                data_transformation_artifact: DataTransformationArtifact):
        self.model_evaluator_config = ModelEvaluatorConfig

        self.model_trainer_artifact = ModelTrainerArtifact

        self.data_transformation_artifact = data_transformation_artifact

    def get_best_model(self, best_model_path:str):
        try:
            logging.info("Entered get_best_model method")
            if not os.path.exists:
                return None

            if len(os.listdir(best_model_path)) == 0:
                return None
            elif len(os.listdir(best_model_path)) > 1:
                raise Exception(f"best model directory: {best_model_path} contains more than one model")
            else:
                trained_model = PipelineModel.load(f"{best_model_path}*")

                return trained_model
            
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def evaluate_model(self, model, test_data: DataFrame):
        try:
            logging.info("Entered evaluate_model method")
            trained_model_acc, trained_model_pred = calculate_classification_metric(model, test_data, ENCODED_TARGET_COL_NAME)

            best_model_acc, best_model_pred = None, None

            best_model = self.get_best_model(self.model_evaluator_config.best_model_path)

            is_accepted = False

            if best_model != None:
                best_model_acc, best_model_pred = calculate_classification_metric(best_model, test_data, ENCODED_TARGET_COL_NAME)

                if best_model_acc < trained_model_acc:
                    is_accepted = True

            return ModelEvaluationResponse(is_accepted, best_model_acc, trained_model_acc, best_model_pred, trained_model_pred)

        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_model_evaluation(self):
        try:
            logging.info("Entered initiate_model_evaluation method")
            test_df = spark_session.read.parquet(f"{self.data_transformation_artifact.test_file_path}*")

            model = PipelineModel.load(f"{self.model_trainer_artifact.trainedmodel_dir_path}*")

            model_eval_response = self.evaluate_model(model, test_df)

            eval_report = model_eval_response.to_dict()

            eval_report = {key : value for key, value in eval_report.items() if key not in [trained_model_pred, best_model_pred]}

            write_yaml_file(self.model_evaluator_config.metric_file_path, eval_report)

            return ModelEvaluatorArtifact(model_eval_response.is_accepted, self.model_evaluator_config.metric_file_path)
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)