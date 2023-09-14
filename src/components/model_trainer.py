import os, sys, importlib

from pyspark.sql import DataFrame
from pyspark.ml.pipeline import Pipeline, PipelineModel

from src.config.spark_manager import spark_session
from src.entity.config_entity import ModelTrainerConfig
from src.entity.artifact_entity import DataTransformationArtifact, ModelTrainerArtifact
from src.constants.training_pipeline import *

from src.logger import logging
from src.exception import UserException

class ModelTrainer:
    def __init__(self, model_trainer_config: ModelTrainerConfig, data_transformation_artifact: DataTransformationArtifact):
        self.model_trainer_config = model_trainer_config

        self.data_transformation_artifact = data_transformation_artifact

    def get_class_from_name(self, module, cls):
        try:
            logging.info("Entered get_class_from_name method")
            new_module = importlib.import_module(module)

            model = getattr(new_module, cls)

            logging.info(f"model passed is {model}")

            return model
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def update_model_params(self, model, **params):
        try:
            logging.info("Entered update_model_params method")
            new_model = model(**params)

            return new_model
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def train_model(self, model, train_data: DataFrame):
        try:
            logging.info("Entered train_model method")
            stages = []

            stages.append(model)

            pipeline = Pipeline(stages=stages)

            trained_model =  pipeline.fit(train_data)
            
            return trained_model
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def save_model(self, model, trained_model_dir_path):
        try:
            logging.info("Entered save_model method")
            #trained_model_name = self.model_trainer_config.model_class
            
            trained_model_path = os.path.join(trained_model_dir_path, TRAINED_MODEL_NAME)

            model.save(trained_model_path)

            logging.info(f"trained model saved to {trained_model_path}")
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)
    
    def initiate_model_training(self)-> ModelTrainerArtifact:
        try:
            logging.info("Entered initiate_model_training method")
            train = spark_session.read.parquet(f"{self.data_transformation_artifact.train_file_path}*")

            model = self.get_class_from_name(self.model_trainer_config.model_module, 
                                            self.model_trainer_config.model_class)

            model_loader = self.get_class_from_name(self.model_trainer_config.model_module, 
                                            self.model_trainer_config.model_loader_class)
            
            model = self.update_model_params(model, **self.model_trainer_config.model_params)

            trained_model = self.train_model(model, train)

            self.save_model(trained_model, self.model_trainer_config.trainedmodel_dir_path)

            logging.info(f"trained model saved to {self.model_trainer_config.trainedmodel_dir_path}")

            return ModelTrainerArtifact(self.model_trainer_config.trainedmodel_dir_path)
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)


if __name__ == '__main__':
    dta = DataTransformationArtifact("./user_exp_artifact/data_transformation/data/train_user_df.parquet", 
                                    "./user_exp_artifact/data_transformation/data/train_user_df.parquet", 
                                    "./user_exp_artifact/data_transformation/object/data_transform_pipeline.pkl")
    mtc = ModelTrainerConfig()
    mt = ModelTrainer(mtc, dta)
    mtc = mt.initiate_model_training()


