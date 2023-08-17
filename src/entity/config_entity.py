import os
from collections import namedtuple
from dataclasses import dataclass
from from_root import from_root

from src.constants.training_pipeline import *

@dataclass
class TrainingPipelineConfig:
    artifacr_dir_path = os.path.join(from_root(), ARTIFACT_DIR)

training_pipeline_config = TrainingPipelineConfig()

@dataclass
class DataIngestionConfig:
    dataingestion_dir_path = os.path.join(training_pipeline_config.artifacr_dir_path, DATA_INGESTION_DIR)

    os.makedirs(dataingestion_dir_path, exist_ok=True)

    uge_data_path = os.path.join(DATA_DOWNLOAD_DIR, UGE_DIR)

    ube_data_path = os.path.join(DATA_DOWNLOAD_DIR, UBE_DIR)

    data_ingested_file_path = os.path.join(dataingestion_dir_path, DATA_INGESTED_FILE_NAME)

@dataclass
class DataValidationConfig:
    datavalidation_dir_path = os.path.join(training_pipeline_config.artifacr_dir_path, DATA_VALIDATION_DIR)

    os.makedirs(datavalidation_dir_path, exist_ok=True)

    data_validated_file_path = os.path.join(datavalidation_dir_path, DATA_VALIDATED_FILE_NAME)

@dataclass
class DataTransformationConfig:
    datatransformation_dir_path = os.path.join(training_pipeline_config.artifacr_dir_path, DATA_TRANSFORMATION_DIR)

    os.makedirs(datatransformation_dir_path, exist_ok=True)

    train_file_path = os.path.join(datatransformation_dir_path, TRANSFORMED_DATA_DIR, TRAIN_FILE_NAME)

    test_file_path = os.path.join(datatransformation_dir_path, TRANSFORMED_DATA_DIR, TEST_FILE_NAME)

    pipeline_file_path = os.path.join(datatransformation_dir_path, TRANSFORMATION_OBJECT_DIR, PIPELINE_FILE_NAME)


    


