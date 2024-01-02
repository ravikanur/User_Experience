import sys, re, os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, hour, minute, second, dayofyear

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset

from src.config.spark_manager import spark_session
from src.config.db_connection import load_data_db
from src.utils.main_utils import rearrange_dataframe_columns
from src.constants.training_pipeline import *
from src.entity.config_entity import DataValidationConfig
from src.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact

from src.logger import logging
from src.exception import UserException

def add_mean_indicator_col_per_user(df: DataFrame, 
                                        groupby_column:str, mean_result_cols:list) -> DataFrame:
        try:
            logging.info(f"Entered add_mean_indicator_col_per_user method")
            final_df = []

            df_all = df

            req_cols = mean_result_cols + [groupby_column]

            df_indicator = df_all.select(*req_cols)

            df_indicator_avg = df_indicator.groupBy(col(groupby_column)).mean()

            ind_avg_list_per_user = df_indicator_avg.collect()

            logging.info(f"length of ind_avg_list_per_user is {len(ind_avg_list_per_user)}")

            col_list = df_indicator_avg.columns[1:]

            for i,row in enumerate(ind_avg_list_per_user):
                temp_df = df_all.filter(col(groupby_column) == row[0])
                for column in col_list:
                    temp_col_name = re.split('\(|\)', column)[1]

                    temp_df = temp_df.withColumn(f"{temp_col_name}_avg", lit(row[column]))
                if i == 0:
                    final_df = temp_df
                else:
                    final_df = final_df.union(temp_df)
            logging.info("Done with adding mean columns per user")

            return final_df
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

class DataValidation:
    def __init__(self, data_validation_config: DataValidationConfig,
                data_ingestion_artifact:  DataIngestionArtifact):
        self.data_validation_config = data_validation_config

        self.data_ingestion_artifact = data_ingestion_artifact

    def detect_data_drift(self, current_data: DataFrame, reference_data: DataFrame):
        try:
            logging.info("Entered detect_data_drift method")
            cur_data = current_data.withColumnRenamed(TARGET_COLUMN_NAME, DRIFT_TARGET_COLUMN_NAME)

            ref_data = reference_data.withColumnRenamed(TARGET_COLUMN_NAME, DRIFT_TARGET_COLUMN_NAME)

            cur_data_pd = cur_data.toPandas()

            ref_data_pd = ref_data.toPandas()

            #data_drift_report
            data_drift_report = Report(metrics=[DataDriftPreset()])

            data_drift_report.run(reference_data=ref_data_pd, current_data=cur_data_pd)

            data_drift_report.save_json(f"{os.path.join(self.data_validation_config.data_drift_report_dir_path, DATA_DRIFT_REPORT_NAME)}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")

            data_drift_report.save_html(f"{os.path.join(self.data_validation_config.data_drift_report_dir_path, DATA_DRIFT_REPORT_NAME)}_{datetime.now().strftime('%Y%m%d%H%M%S')}.html")

            data_drift_dict = data_drift_report.as_dict()

            #target_drift_report
            target_drift_report = Report(metrics=[TargetDriftPreset()])

            target_drift_report.run(reference_data=ref_data_pd, current_data=cur_data_pd)

            target_drift_report.save_json(f"{os.path.join(self.data_validation_config.target_drift_report_dir_path, TARGET_DRIFT_REPORT_NAME)}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")

            target_drift_report.save_html(f"{os.path.join(self.data_validation_config.target_drift_report_dir_path, TARGET_DRIFT_REPORT_NAME)}_{datetime.now().strftime('%Y%m%d%H%M%S')}.html")

            target_drift_dict = target_drift_report.as_dict()

            if target_drift_dict['metrics'][0]['result']['drift_detected'] or data_drift_dict['metrics'][0]['result']['dataset_drift']:
                logging.info(f"Drift detected. Below are the details.\ntarget_drift:{target_drift_dict}\ndata_drift:{data_drift_dict}")

                #raise Exception(" Drift detected in new data. Hence stopping the training.")

        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_data_validation(self)-> DataValidationArtifact:
        try:
            logging.info("Entered initiate_data_validation method")
            user_df = spark_session.read.csv(f"{self.data_ingestion_artifact.data_ingested_file_path}*", 
                                            header=True, inferSchema=True)

            for column in INDICATOR_COLS:
                user_df = user_df.filter(col(column) < INDICATOR_THRESHOLD)

            logging.info(f"count of df after removal of outliers is {user_df.count()}")

            user_df = user_df.withColumn(MINUTE_COL, minute(col(DATE_VAL_COL)))\
                            .withColumn(SECOND_COL, second(col(DATE_VAL_COL)))\
                            .withColumn('day', dayofyear(col(DATE_VAL_COL)))

            user_df = add_mean_indicator_col_per_user(user_df, USER_COLUMN_NAME, INDICATOR_COLS)
            logging.info(user_df.show())

            ref_df = load_data_db(TRAINING_DB_TABLE_NAME)

            ref_df_flag: bool = False

            if ref_df is not None:
                ref_df = ref_df.withColumn(MINUTE_COL, minute(col(DATE_VAL_COL)))\
                                .withColumn(SECOND_COL, second(col(DATE_VAL_COL)))\
                                .withColumn('day', dayofyear(col(DATE_VAL_COL)))\
                                .withColumn('hour', hour(col(DATE_VAL_COL)))\
                                .drop('submit_date')
                logging.info(ref_df.show())
                
                ref_df1 = ref_df.drop(DATE_VAL_COL)

                user_df1 = user_df.drop(DATE_VAL_COL)
                
                self.detect_data_drift(user_df1, ref_df1)

                user_df.write.mode('overwrite').parquet(self.data_validation_config.data_validated_file_db_path)

                ref_df = rearrange_dataframe_columns(user_df, ref_df)
                logging.info(f"user_df columns: {user_df.columns}")
                logging.info(f"ref_df columns: {ref_df.columns}")
                user_df = user_df.union(ref_df)

                ref_df_flag = True

            #user_df_db = user_df.drop(*[COLS_TO_BE_REMOVED_DB])

            #user_df = user_df.drop(*COLS_TO_BE_REMOVED)

            user_df.write.mode('overwrite').parquet(self.data_validation_config.data_validated_file_path)

            return DataValidationArtifact(self.data_validation_config.data_validated_file_path, 
                                        self.data_validation_config.data_validated_file_db_path, ref_df_flag)

        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

if __name__ == '__main__':
    data_validation_config = DataValidationConfig()
    data_ingestion_artifact = DataIngestionArtifact('./user_exp_artifact/data_ingestion/User_Experience_data.csv')
    data_validation = DataValidation(data_validation_config, data_ingestion_artifact)
    data_validation.initiate_data_validation()

    
                 
