##common
ARTIFACT_DIR: str = 'user_exp_artifact'

## data_ingestion
DATA_S3_URL:str = ''

TARGET_COLUMN_NAME:str = 'result'

USER_COLUMN_NAME:str = 'user'

DATA_INGESTION_DIR = 'data_ingestion'

DATA_DOWNLOAD_DIR = 'user_downloaded_data'

UGE_DIR: str = 'UGE'

UBE_DIR: str = 'UBE'

DATA_INGESTED_FILE_NAME: str = 'User_Experience_data.csv'

## data_validation
DATA_VALIDATION_DIR: str = 'data_validation'

DATE_VAL_STRING: str = 'specifictime'

INDICATOR_THRESHOLD: int = 2000

INDICATOR_COLS:list = ['indicator1', 'indicator2', 'indicator3', 'indicator4', 'indicator5', 
                        'indicator6', 'indicator7', 'indicator8']

DATA_VALIDATED_FILE_NAME: str = 'User_final_data.csv'

COLS_TO_BE_REMOVED:list = ['_c0','day', 'hour']

## data_transformation
DATA_TRANSFORMATION_DIR: str = 'data_transformation'

TRANSFORMED_DATA_DIR:str = 'data'

TRANSFORMATION_OBJECT_DIR:str = 'object'

TRAIN_FILE_NAME: str = 'train_user_df.csv'

TEST_FILE_NAME: str = 'test_user_df.csv'

VALID_FILE_NAME: str = 'valid_user_df.csv'

PIPELINE_FILE_NAME: str = 'data_transform_pipeline.pkl'

LABEL_FEATURES:list = ['specifictime']

SCALAR_FEATURES: list = ['indicator1', 'indicator2', 'indicator3', 'indicator4', 'indicator5', 
                        'indicator6', 'indicator7', 'indicator8', 'indicator1_avg', 'indicator2_avg',
                         'indicator3_avg', 'indicator4_avg', 'indicator5_avg', 'indicator6_avg', 
                         'indicator7_avg', 'indicator8_avg']

FEATURE_COLS_NAME: str = 'feature'

ENCODED_TARGET_COL_NAME:str = 'label'

FINAL_COLS: list = []

