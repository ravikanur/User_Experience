import os, sys, time

from pyspark.sql import dataframe
from pyspark.sql.functions import lit, col
from pyspark.ml.pipeline import PipelineModel
from src.config.spark_manager import spark_session
from src.constants.prediction_pipeline import *
from src.constants.training_pipeline import *
from src.components.data_validation import add_mean_indicator_col_per_user

from src.utils.main_utils import read_yaml_file

from src.logger import logging
from src.exception import UserException

class PredictionPipeline:
    def read_data(self, path:str):
        try:
            logging.info("Entered read_data method")
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)
    def get_valid_invalid_files(self):
        try:
            logging.info("Entered get_valid_invalid_files method")
            user_list = os.listdir(INPUT_DIR)

            user_name_list = []

            valid_files, invalid_files = [], []

            for i, user in enumerate(user_list):
                user_path = os.path.join(INPUT_DIR, user)

                user_name = user.split(sep='.')[0]

                user_name_list.append(user_name)

                temp_df: dataframe = spark_session.read.csv(user_path, inferSchema=True, header=True)

                cols = temp_df.columns

                no_col_list = [col for col in REQUIRED_COLUMNS if col not in cols]

                if len(no_col_list) > 0:
                    invalid_files.append(user)
                else:
                    valid_files.append(user)
                    
                    temp_df = temp_df.withColumn(USER_COLUMN_NAME, lit(user_name))
                    if i == 0:
                        final_df = temp_df
                    else:
                        final_df = final_df.union(temp_df)
            return valid_files, invalid_files, final_df, user_name_list
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)
    def initiate_batch_prediction(self):
        try:
            logging.info("Entered initiate_batch_prediction method")
            start_time = time.time()
            valid_files, invalid_files, final_df, user_name_list = self.get_valid_invalid_files()

            for column in INDICATOR_COLS:
                final_df = final_df.filter(col(column) < INDICATOR_THRESHOLD)

            final_df = final_df.withColumn(DATE_VAL_STRING, col(DATE_VAL_STRING).cast('string'))

            final_df = add_mean_indicator_col_per_user(final_df, USER_COLUMN_NAME, INDICATOR_COLS)

            final_df = final_df.drop(*COLS_TO_BE_REMOVED)

            #final_df = final_df.withColumn(TARGET_COLUMN_NAME, lit('UBE'))

            model = PipelineModel.load(PRED_MODEL_PATH)

            for i, user_name in enumerate(user_name_list):
                temp_df = final_df.filter(col(USER_COLUMN_NAME) == user_name)

                pred = model.transform(temp_df)

                target_mapping = read_yaml_file(TARGET_MAPPING_FILE_PATH)

                pred1 = pred.select('prediction').rdd.map(lambda x: (target_mapping[x.prediction], )).toDF(['prediction', ])

                #pred1 = pred1.dropDuplicates()
                pred1 = pred1.groupBy('prediction').count().sort(col('count').desc())

                if pred1.count() > 1:
                    pred1 = pred1.filter(pred1.prediction == pred1.first()[0]).select('prediction')
                elif pred1.count() == 1:
                    pred1 = pred1.select('prediction')
                else:
                    pass

                pred1 = pred1.withColumn(USER_COLUMN_NAME, lit(user_name))
                if i == 0:
                    pred_df = pred1
                else:
                    pred_df = pred_df.union(pred1)

            end_time = time.time()
            #pred1.write.mode('append').parquet('./output/pred.parquet')
            pred_df.coalesce(1).write.mode('append').csv('./output/pred.csv', header=True)
            logging.info(f"Total prediction time took to predict {i} users is {round((end_time - start_time), 2)}")
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

if __name__ == '__main__':
    pred_pipeline = PredictionPipeline()
    pred_pipeline.initiate_batch_prediction()