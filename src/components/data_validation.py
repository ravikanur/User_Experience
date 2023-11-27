import sys, re
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col

from evidently.report import Report

from src.config.spark_manager import spark_session
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

    def initiate_data_validation(self)-> DataValidationArtifact:
        try:
            logging.info("Entered initiate_data_validation method")
            user_df = spark_session.read.csv(f"{self.data_ingestion_artifact.data_ingested_file_path}*", 
                                            header=True, inferSchema=True)
            

            for column in INDICATOR_COLS:
                user_df = user_df.filter(col(column) < INDICATOR_THRESHOLD)

            user_df = user_df.withColumn(DATE_VAL_STRING, col(DATE_VAL_STRING).cast('string'))

            logging.info(f"count of df after removal of outliers is {user_df.count()}")

            user_df = add_mean_indicator_col_per_user(user_df, USER_COLUMN_NAME, INDICATOR_COLS)

            user_df = user_df.drop(*COLS_TO_BE_REMOVED)

            user_df.write.mode('append').csv(self.data_validation_config.data_validated_file_path, header=True)

            return DataValidationArtifact(self.data_validation_config.data_validated_file_path)

        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

if __name__ == '__main__':
    data_validation_config = DataValidationConfig()
    data_ingestion_artifact = DataIngestionArtifact('./user_exp_artifact/data_ingestion/User_Experience_data.csv')
    data_validation = DataValidation(data_validation_config, data_ingestion_artifact)
    data_validation.initiate_data_validation()

    
                 
