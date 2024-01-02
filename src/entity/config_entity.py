import os
from collections import namedtuple
from dataclasses import dataclass
from from_root import from_root

from src.constants.training_pipeline import *
from src.utils.main_utils import read_yaml_file

@dataclass
class TrainingPipelineConfig:
    artifacr_dir_path = os.path.join(from_root(), ARTIFACT_DIR)

    os.makedirs(artifacr_dir_path, exist_ok=True)

    model_dir_path = os.path.join(from_root(), MODEL_DIR)

    os.makedirs(model_dir_path, exist_ok=True)

    drift_report_dir_path = os.path.join(from_root(), DRIFT_REPORT_DIR)

    os.makedirs(drift_report_dir_path, exist_ok=True)

    config_file_path = os.path.join(from_root(), CONFIG_DIR, CONFIG_FILE)

    model_config_file_path = os.path.join(from_root(), CONFIG_DIR, MODEL_CONFIG_FILE)

    config = read_yaml_file(config_file_path)

    model_config = read_yaml_file(model_config_file_path)

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

    data_validated_file_db_path = os.path.join(datavalidation_dir_path, DATA_VALIDATED_FILE_DB_NAME)

    data_drift_report_dir_path = os.path.join(training_pipeline_config.drift_report_dir_path, DATA_DRIFT_REPORT_DIR)

    os.makedirs(data_drift_report_dir_path, exist_ok=True)

    target_drift_report_dir_path = os.path.join(training_pipeline_config.drift_report_dir_path, TARGET_DRIFT_REPORT_DIR)

    os.makedirs(target_drift_report_dir_path, exist_ok=True)

@dataclass
class DataTransformationConfig:
    datatransformation_dir_path = os.path.join(training_pipeline_config.artifacr_dir_path, DATA_TRANSFORMATION_DIR)

    os.makedirs(datatransformation_dir_path, exist_ok=True)

    train_file_path = os.path.join(datatransformation_dir_path, TRANSFORMED_DATA_DIR, TRAIN_FILE_NAME)

    test_file_path = os.path.join(datatransformation_dir_path, TRANSFORMED_DATA_DIR, TEST_FILE_NAME)

    pipeline_file_path = os.path.join(datatransformation_dir_path, TRANSFORMATION_OBJECT_DIR, PIPELINE_FILE_NAME)

    target_mapping_file_path = os.path.join(CONFIG_DIR, TARGET_MAPPING_FILE_NAME)

@dataclass
class ModelTrainerConfig:
    modeltrainer_dir_path = os.path.join(training_pipeline_config.artifacr_dir_path, MODEL_TRAINER_DIR)

    os.makedirs(modeltrainer_dir_path, exist_ok=True)
    
    trainedmodel_dir_path = os.path.join(training_pipeline_config.model_dir_path, TRAINED_MODEL_DIR)

    os.makedirs(trainedmodel_dir_path, exist_ok=True)

    _model_ref = training_pipeline_config.config[MODEL_CONFIG][MODEL]

    model_module = training_pipeline_config.model_config[MODEL_SELECTION][_model_ref][MODEL_MODULE]

    model_class = training_pipeline_config.model_config[MODEL_SELECTION][_model_ref][MODEL_CLASS]

    model_params = training_pipeline_config.model_config[MODEL_SELECTION][_model_ref][MODEL_PARAMS]

    model_loader_class = training_pipeline_config.model_config[MODEL_SELECTION][_model_ref][MODEL_LOADER]

    #model_fit_params = training_pipeline_config.model_config[MODEL_SELECTION][_model_ref][MODEL_FIT_PARAMS]

@dataclass
class ModelEvaluatorConfig:
    bestmodel_dir_path = os.path.join(training_pipeline_config.model_dir_path, BEST_MODEL_DIR_NAME)

    os.makedirs(bestmodel_dir_path, exist_ok=True)

    metric_dir_path = os.path.join(training_pipeline_config.model_dir_path, METRICS_DIR_NAME)

    os.makedirs(metric_dir_path, exist_ok=True)

    metric_file_path = os.path.join(metric_dir_path, METRICS_FILE_NAME)

@dataclass
class ModelPusherConfig:
    trainedmodels_dir_path = os.path.join(training_pipeline_config.model_dir_path, TRAINED_MODELS_DIR)

    os.makedirs(trainedmodels_dir_path, exist_ok=True)


    








    


