INPUT_DIR: str = 'input'

PREDICTION_DIR: str = 'prediction'

DB_COLUMNS: list = ['specifictime','indicator1','indicator2','indicator3','indicator4',
                            'indicator5','indicator6','indicator7','indicator8', 'user', 'prediction',
                             'indicator1_avg', 'indicator2_avg', 'indicator3_avg', 'indicator4_avg', 
                             'indicator5_avg','indicator6_avg', 'indicator7_avg', 'indicator8_avg']

REQUIRED_COLUMNS: list = ['specifictime','indicator1','indicator2','indicator3','indicator4',
                            'indicator5','indicator6','indicator7','indicator8']

PRED_MODEL_PATH: str = "./model/best_model/UE_model"

TARGET_MAPPING_FILE_PATH: str = './config/target_column_mapping.yaml'

PREDICTION_DB_TABLE_NAME: str = 'pred_data'
