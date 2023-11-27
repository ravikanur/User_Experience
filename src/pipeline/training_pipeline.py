import os, sys

from src.entity.config_entity import (DataIngestionConfig, DataValidationConfig, 
                                    DataTransformationConfig, ModelTrainerConfig,
                                    ModelEvaluatorConfig, ModelPusherConfig, TrainingPipelineConfig)
from src.entity.artifact_entity import (DataIngestionArtifact, DataValidationArtifact,
                                    DataTransformationArtifact, ModelTrainerArtifact,
                                    ModelEvaluatorArtifact)
from src.components.data_ingestion import DataIngestion
from src.components.data_validation import DataValidation
from src.components.data_transformation import DataTransformation
from src.components.model_trainer import ModelTrainer
from src.components.model_evaluator import ModelEvaluator
from src.components.model_pusher import ModelPusher

from src.logger import logging
from src.exception import UserException

class TrainingPipeline:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.training_pipeline_config = training_pipeline_config

    def initiate_data_ingestion(self, data_ingestion_config: DataIngestionConfig)-> DataIngestionArtifact:
        try:
           data_ingestion = DataIngestion(data_ingestion_config)

           data_ingestion_artifact = data_ingestion.initiate_data_ingestion()

           return data_ingestion_artifact
        except UserException as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_data_validation(self, data_validation_config: DataValidationConfig, 
                                data_ingestion_artifact: DataIngestionArtifact)-> DataValidationArtifact:
        try:
            data_validation = DataValidation(data_validation_config, data_ingestion_artifact)

            data_validation_artifact = data_validation.initiate_data_validation()

            return data_validation_artifact
        except UserException as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_data_transformation(self, data_transformation_config: DataTransformationConfig, 
                                        data_validation_artifact: DataValidationArtifact)-> DataTransformationArtifact:
        try:
            data_transformation = DataTransformation(data_transformation_config, data_validation_artifact)

            data_transformation_artifact = data_transformation.initiate_data_transformation()

            return data_transformation_artifact
        except UserException as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_model_trainer(self, model_trainer_config: ModelTrainerConfig, 
                                data_transformation_artifact: DataTransformationArtifact)-> ModelTrainerArtifact:
        try:
            model_trainer = ModelTrainer(model_trainer_config, data_transformation_artifact)

            model_trainer_artifact = model_trainer.initiate_model_training()

            return model_trainer_artifact
        except UserException as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_model_evaluation(self, model_evaluation_config: ModelEvaluatorConfig, 
                                        model_trainer_artifact: ModelTrainerArtifact,
                                        data_transformation_artifact: DataTransformationArtifact)-> ModelEvaluatorArtifact:
        try:
            model_evaluate = ModelEvaluator(model_evaluation_config, model_trainer_artifact, data_transformation_artifact)

            model_evaluation_artifact = model_evaluate.initiate_model_evaluation()

            return model_evaluation_artifact
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_model_pusher(self, model_evaluation_config: ModelEvaluatorConfig, 
                            model_trainer_config: ModelTrainerConfig,
                            model_trainer_artifact: ModelTrainerArtifact):
        try:
            model_pusher = ModelPusher(model_evaluation_config, model_trainer_config, model_trainer_artifact)

            model_pusher_artifact = model_pusher.initiate_model_pushing()
        except UserException as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_training_pipeline(self)-> None:
        try:
            logging.info("Entered initiate_training_pipeline method")

            data_ingestion_config = DataIngestionConfig()
            data_ingestion_artifact = self.initiate_data_ingestion(data_ingestion_config)

            data_validation_config = DataValidationConfig()
            data_validation_artifact = self.initiate_data_validation(data_validation_config, data_ingestion_artifact)

            data_transformation_config = DataTransformationConfig()
            data_transformation_artifact = self.initiate_data_transformation(data_transformation_config, data_validation_artifact)

            model_trainer_config = ModelTrainerConfig()
            model_trainer_artifact = self.initiate_model_trainer(model_trainer_config, data_transformation_artifact)

            model_evaluation_config = ModelEvaluatorConfig()
            model_evaluator_artifact = self.initiate_model_evaluation(model_evaluation_config, model_trainer_artifact, data_transformation_artifact)

            if model_evaluator_artifact.is_accepted == False:
                raise Exception("Trained model is not accepted")
            
            model_pusher_config = ModelPusherConfig()
            self.initiate_model_pusher(model_evaluation_config, model_pusher_config, model_trainer_config)
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

if __name__ == "__main__":
    tpc = TrainingPipelineConfig()
    tp = TrainingPipeline(tpc)
    tp.initiate_training_pipeline()
