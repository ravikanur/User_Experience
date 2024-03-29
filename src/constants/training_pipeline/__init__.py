##common
ARTIFACT_DIR: str = 'user_exp_artifact'

MODEL_DIR: str = 'model'

CONFIG_DIR: str = 'config'

DRIFT_REPORT_DIR:str = 'Drift_Report'

CONFIG_FILE: str = 'config.yaml'

MODEL_CONFIG_FILE: str ='model_config.yaml'

TRAINING_DB_TABLE_NAME: str = 'train_data'

## data_ingestion
DATA_S3_URL:str = ''

TARGET_COLUMN_NAME:str = 'result'

USER_COLUMN_NAME:str = 'user'

DATA_INGESTION_DIR = 'data_ingestion'

DATA_DOWNLOAD_DIR = 'user_download_data'

UGE_DIR: str = 'UGE'

UBE_DIR: str = 'UBE'

DATA_INGESTED_FILE_NAME: str = 'User_Experience_data.csv'

## data_validation
DATA_VALIDATION_DIR: str = 'data_validation'

DATE_VAL_COL: str = 'specifictime'

SECOND_COL:str = 'second'

MINUTE_COL:str = 'minute'

INDICATOR_THRESHOLD: int = 2000

INDICATOR_COLS:list = ['indicator1', 'indicator2', 'indicator3', 'indicator4', 'indicator5', 
                        'indicator6', 'indicator7', 'indicator8']

DATA_VALIDATED_FILE_NAME: str = 'User_final_data.parquet'

DATA_VALIDATED_FILE_DB_NAME: str = 'User_final_data_db.parquet'

COLS_TO_BE_REMOVED_DB:list = ['_c0','day', 'hour', MINUTE_COL, SECOND_COL]

DRIFT_TARGET_COLUMN_NAME:str = 'target'

DATA_DRIFT_REPORT_DIR:str = 'data_drift'

TARGET_DRIFT_REPORT_DIR:str = 'target_drift'

DATA_DRIFT_REPORT_NAME:str = 'data_drift_report'

TARGET_DRIFT_REPORT_NAME:str = 'target_drift_report'

## data_transformation
COLS_TO_BE_REMOVED:list = ['_c0', 'specifictime']

DATA_TRANSFORMATION_DIR: str = 'data_transformation'

TRANSFORMED_DATA_DIR:str = 'data'

TRANSFORMATION_OBJECT_DIR:str = 'object'

TRAIN_FILE_NAME: str = 'train_user_df.parquet'

TEST_FILE_NAME: str = 'test_user_df.parquet'

VALID_FILE_NAME: str = 'valid_user_df.parquet'

PIPELINE_FILE_NAME: str = 'data_transform_pipeline.pkl'

TARGET_MAPPING_FILE_NAME: str = 'target_column_mapping.yaml'

#LABEL_FEATURES:list = ['specifictime']

SCALAR_FEATURES: list = ['minute', 'indicator1', 'indicator2', 'indicator3', 'indicator4', 'indicator5', 
                        'indicator6', 'indicator7', 'indicator8', 'indicator1_avg', 'indicator2_avg',
                         'indicator3_avg', 'indicator4_avg', 'indicator5_avg', 'indicator6_avg', 
                         'indicator7_avg', 'indicator8_avg', 'second', 'day', 'hour']

FEATURE_COLS_NAME: str = 'feature'

ENCODED_TARGET_COL_NAME:str = 'label'

## model_training
MODEL_TRAINER_DIR: str = 'model_trainer'

TRAINED_MODEL_DIR: str = 'trained_models'

TRAINED_MODEL_NAME: str = 'UE_model'

BASE_TRAINING_SCORE: float = 0.7

# config yaml params
MODEL_CONFIG: str  = 'model_config'

MODEL: str = 'model'

 # model yaml params
MODEL_SELECTION: str = 'model_selection'

MODEL_CLASS: str = 'class'

MODEL_MODULE: str = 'module'

MODEL_LOADER: str = 'loader'

MODEL_PARAMS: str = 'params'

MODEL_FIT_PARAMS: str = 'fit_params'

MODEL_FIT_PARAMS_VERBOSE: str = 'verbose'

MODEL_FIT_PARAMS_EVAL_SET: str = 'eval_set'

# model evaluation parameters

METRICS_DIR_NAME: str = 'metrics'

BEST_MODEL_DIR_NAME: str = 'best_model'

METRICS_FILE_NAME: str = 'metrics.yaml'

TEST_PRED_FILE_NAME: str = 'User_test_pred.csv'

THRESHOLD_SCORE: float = 0.85

# model pusher parameters

TRAINED_MODELS_DIR: str = 'trained_models'
