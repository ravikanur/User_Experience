import os, sys

from src.entity.config_entity import (DataIngestionConfig, DataValidationConfig, 
                                    DataTransformationConfig, ModelTrainerConfig,
                                    ModelEvaluationConfig, TrainingPipelineConfig)
from src.entity.artifact_entity import (DataIngestionArtifact, DataValidationArtifact,
                                    DataTransformationArtifact, ModelTrainerArtifact,
                                    ModelEvaluationArtifact)
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

    def initiate_model_evaluation(self, model_evaluation_config: ModelEvaluationConfig, 
                                        model_trainer_artifact: ModelTrainerArtifact,
                                        data_transformation_artifact: DataTransformationArtifact)-> ModelEvaluationArtifact:
        try:
            model_evaluate = ModelEvaluator(model_evaluation_config, model_trainer_artifact, data_transformation_artifact)
