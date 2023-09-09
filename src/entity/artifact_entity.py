from collections import namedtuple

DataIngestionArtifact = namedtuple("DataIngestionArtifact", "data_ingested_file_path")

DataValidationArtifact = namedtuple("DataValidationArtifact", "data_validated_file_path")

DataTransformationArtifact = namedtuple("DataTransformationArtifact", ["train_file_path", "test_file_path", "pipeline_file_path"])

ModelTrainerArtifact = namedtuple("ModelTrainerArtifact", ["trainedmodel_dir_path"])

ModelEvaluatorArtifact = namedtuple("ModelEvaluatorArtifact", ["is_accepted", "metric_file_path"])

ModelEvaluationResponse = namedtuple("ModelEvaluationResponse", ["is_accepted", "best_model_score", "trained_model_score", "best_model_pred", "trained_model_pred"])

