import os, sys, re

from pyspark.sql import dataframe
from pyspark.sql.functions import lit

from src.config.spark_manager import spark_session
from src.constants.training_pipeline import *
from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact

from src.logger import logging
from src.exception import UserException

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        self.data_ingestion_config = data_ingestion_config

    def read_downloaded_data(self, paths:list):
        try:
            logging.info("Entered read_downloaded_data method")
            for i, path in enumerate(paths):
                file_list = os.listdir(path)
                for j,file in enumerate(file_list):
                    file_path = os.path.join(path, file)

                    user = file.split(sep='.')[0]

                    user_type = re.split('/', path)[-1]

                    temp_df = spark_session.read.csv(file_path, header=True, inferSchema=True)

                    temp_df = temp_df.withColumn(USER_COLUMN_NAME, lit(f"{user}_{user_type}"))
                    if j == 0:
                        temp_df1 = temp_df
                    else:
                        temp_df1 = temp_df1.union(temp_df)
                temp_df1 = temp_df1.withColumn(TARGET_COLUMN_NAME, lit(f"{user_type}"))
                if i == 0:    
                    temp_df2 = temp_df1
                else:
                    temp_df2 = temp_df2.union(temp_df1)

            logging.info(f"reading of CSV is done")

            return temp_df2     
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact :
        try:
            logging.info("Entered initiate_data_ingestion method")
            user_df: dataframe = self.read_downloaded_data([self.data_ingestion_config.ube_data_path, 
                                                self.data_ingestion_config.uge_data_path])
            
            user_df.write.mode('overwrite').csv(self.data_ingestion_config.data_ingested_file_path, header=True)

            logging.info(f"dataframe has been saved in path {self.data_ingestion_config.data_ingested_file_path}")

            DataIngestionArtifact.data_ingested_file_path = self.data_ingestion_config.data_ingested_file_path
            
            return DataIngestionArtifact
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

if __name__ == '__main__':
    data_ingestion_config = DataIngestionConfig()
    data_ingestion = DataIngestion(data_ingestion_config)
    data_ingestion.initiate_data_ingestion()




