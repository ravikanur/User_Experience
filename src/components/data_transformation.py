import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, StandardScaler, VectorAssembler
from pyspark.ml.pipeline import Pipeline

from src.config.spark_manager import spark_session
from src.entity.config_entity import DataTransformationConfig
from src.entity.artifact_entity import DataValidationArtifact, DataTransformationArtifact
from src.constants.training_pipeline import *

from src.utils.main_utils import write_yaml_file

from src.logger import logging
from src.exception import UserException

class DataTransformation:
    def __init__(self, data_transformation_config: DataTransformationConfig,
                data_validation_artifact: DataValidationArtifact):
        self.data_transformation_config = data_transformation_config

        self.data_validation_artifact = data_validation_artifact

    def encode_target_data(self, data: DataFrame, inputcol: str, outputcol:str) -> DataFrame:
        try:
            logging.info("Entered encode_target_data method")
            string_indexer = StringIndexer(inputCol=inputcol, outputCol=outputcol)

            res_df = string_indexer.fit(data).transform(data)

            return res_df
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def write_target_mapping(self, data: DataFrame, key_col:str, val_col:str):
        try:
            logging.info("Entered write_traget_mapping method")
            map_df_list = data.select(*[key_col, val_col]).dropDuplicates().collect()

            pair = {}

            for df_row in map_df_list:
                pair[df_row[0]] = df_row[1]
            
            write_yaml_file(self.data_transformation_config.target_mapping_file_path, pair)

            logging.info(f"target column mapping has been written in path: {self.data_transformation_config.target_mapping_file_path}")
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def transform_data(self, train: DataFrame):
        try:
            logging.info("Entered transform_data method")
            stages = []

            #string_indexer1 = StringIndexer(inputCols=LABEL_FEATURES, outputCols=[f"en_{col}" for col in LABEL_FEATURES])
            
            #stages.append(string_indexer1)
            
            vector_assembler = VectorAssembler(inputCols=SCALAR_FEATURES, outputCol="num_features")
            
            stages.append(vector_assembler)
            
            scalar = StandardScaler(inputCol=vector_assembler.getOutputCol(), outputCol=FEATURE_COLS_NAME)
            
            stages.append(scalar)
            
            #string_indexer3 = StringIndexer(inputCol=TARGET_COLUMN_NAME, outputCol=ENCODED_TARGET_COL_NAME)
            
            #stages.append(string_indexer3)
            
            pipeline = Pipeline(stages=stages)

            transformed = pipeline.fit(train)

            train_transformed = transformed.transform(train)

            #test_transformed = transformed.transform(test)

            logging.info("transformation of train and test data done.")

            return train_transformed, transformed
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def prepare_train_test_data(self, data: DataFrame, train_percentage:float, 
                                categorical_cols: list)-> DataFrame:
        try:
            logging.info("Entered prepare_train_test_data method")
            #train, test = data.randomSplit([train_percentage, 1 - train_percentage], seed=42)
            train, test = data.randomSplit([train_percentage, 1 - train_percentage])

            empty_rdd = spark_session.sparkContext.emptyRDD()

            temp_df_1 = spark_session.createDataFrame(empty_rdd, schema=train.schema)

            for column in categorical_cols:
                cat_train_df = train.select(col(column))

                cat_test_df = test.select(col(column))

                df_diff = cat_test_df.subtract(cat_train_df).collect()

                logging.info(f"column {column} in test dataset has {len(df_diff)} values not present in train dataset")

                if len(df_diff) > 0:
                    for row in df_diff:
                        temp_df = test.where(col(column) == row[column]).dropDuplicates([column])
                        temp_df_1 = temp_df_1.union(temp_df)
            
            if temp_df_1.count() > 0:
                train = train.union(temp_df_1)

            logging.info(f"train and test split done. train count is {train.count()}, test count is {test.count()}")
            
            return train, test
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_data_transformation(self)-> DataTransformationArtifact:
        try:
            logging.info("Entered initiate_data_transformation method")
            user_df = spark_session.read.parquet(f"{self.data_validation_artifact.data_validated_file_path}*")

            user_df = self.encode_target_data(user_df, TARGET_COLUMN_NAME, ENCODED_TARGET_COL_NAME)

            self.write_target_mapping(user_df, ENCODED_TARGET_COL_NAME, TARGET_COLUMN_NAME)

            train, test = self.prepare_train_test_data(user_df, 0.7, [TARGET_COLUMN_NAME])

            train_transformed, preprocessor = self.transform_data(train)

            train_transformed = train_transformed.select(*[FEATURE_COLS_NAME, ENCODED_TARGET_COL_NAME])

            train_transformed.write.mode('overwrite').parquet(self.data_transformation_config.train_file_path)

            test.write.mode('overwrite').parquet(self.data_transformation_config.test_file_path)

            preprocessor.write().overwrite().save(self.data_transformation_config.pipeline_file_path)

            logging.info(f"processed and saved train to {self.data_transformation_config.train_file_path}\n\
                        test to {self.data_transformation_config.test_file_path}\n\
                        preprocessor to {self.data_transformation_config.pipeline_file_path}")

            return DataTransformationArtifact(self.data_transformation_config.train_file_path, 
                                            self.data_transformation_config.test_file_path, 
                                            self.data_transformation_config.pipeline_file_path)
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

if __name__ == '__main__':
    dva = DataValidationArtifact("./user_exp_artifact/data_validation/User_final_data.csv")
    dtc = DataTransformationConfig()
    dt = DataTransformation(dtc, dva)
    dta = dt.initiate_data_transformation()

