import os, sys, shutil
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.ml.pipeline import PipelineModel

from src.config.spark_manager import spark_session
from src.constants.training_pipeline import *
from src.entity.config_entity import ModelEvaluatorConfig, ModelTrainerConfig
from src.entity.artifact_entity import ModelTrainerArtifact

from src.logger import logging
from src.exception import UserException

class ModelPusher:
    def __init__(self, model_evaluator_config: ModelEvaluatorConfig,
                model_trainer_config: ModelTrainerConfig,
                model_trainer_artifact: ModelTrainerArtifact):
        self.model_evaluator_config = model_evaluator_config

        self.model_trainer_config = model_trainer_config

        self.model_trainer_artifact = model_trainer_artifact

    def backup_existing_model(self, curr_dir_path, dest_dir_path):
        try:
            logging.info("Entered get_best_model method")
            if len(os.listdir(curr_dir_path)) == 0:
                return None
            elif len(os.listdir(curr_dir_path)) > 1:
                raise Exception("best_model_dir contains more than one model")
            else:
                model = os.listdir(curr_dir_path)[0]

                new_model = f"{model}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl"

                new_model_path = os.path.join(curr_dir_path, new_model)

                os.rename(os.path.join(curr_dir_path, model), new_model_path)

                shutil.copytree(new_model_path, dest_dir_path)

                return new_model_path
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_model_pushing(self):
        try:
            logging.info("Entered initiate_model_pushing method")
            trained_model_dir_path = self.model_trainer_config.trainedmodel_dir_path

            trained_model_path = self.model_trainer_artifact.trainedmodel_dir_path

            best_model_dir_path = self.model_evaluator_config.bestmodel_dir_path

            new_model_path = self.backup_existing_model(best_model_dir_path, trained_model_dir_path)

            shutil.copytree(trained_model_path, best_model_dir_path, ignore_dangling_symlinks=True)
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)