import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, StandardScaler, VectorAssembler
from pyspark.ml.pipeline import Pipeline

from src.config.spark_manager import spark_session
from src.entity.config_entity import DataTransformationConfig
from src.entity.artifact_entity import DataValidationArtifact, DataTransformationArtifact
from src.constants.training_pipeline import *

from src.logger import logging
from src.exception import UserException

class DataTransformation:
    def __init__(self, data_transformation_config: DataTransformationConfig,
                data_validation_artifact: DataValidationArtifact):
        self.data_transformation_config = data_transformation_config

        self.data_validation_artifact = data_validation_artifact

    def transform_data(self, df: DataFrame, train_percentage:float):
        try:
            logging.info("Entered transform_data method")
            stages = []

            string_indexer1 = StringIndexer(inputCols=LABEL_FEATURES, outputCols=[f"en_{col}" for col in LABEL_FEATURES])
            
            stages.append(string_indexer1)
            
            vector_assembler = VectorAssembler(inputCols=SCALAR_FEATURES + string_indexer1.getOutputCols(), outputCol="num_features")
            
            stages.append(vector_assembler)
            
            scalar = StandardScaler(inputCol=vector_assembler.getOutputCol(), outputCol=FEATURE_COLS_NAME)
            
            stages.append(scalar)
            
            string_indexer3 = StringIndexer(inputCol=TARGET_COLUMN_NAME, outputCol=ENCODED_TARGET_COL_NAME)
            
            stages.append(string_indexer3)
            
            pipeline = Pipeline(stages=stages)

            train, test = df.randomSplit([train_percentage, 1 - train_percentage])

            transformed = pipeline.fit(train)
            transformed.save()

            train_transformed = transformed.transform(train)

            test_transformed = transformed.transform(test)

            return train_transformed, test_transformed, transformed
        except Exception as e:
            logging.error(e)
            raise UserException(e, sys)

    def initiate_data_transformation(self)-> DataTransformationArtifact:
        try:
            logging.info("Entered initiate_data_transformation method")
            user_df = spark_session.read.csv(f"self.{data_validation_artifact.data_validated_file_path}*", header=True, inferSchema=True)

            train, test, preprocessor = self.transform_data(user_df, 0.7)

            train , test = train.select(*[FEATURE_COLS_NAME, ENCODED_TARGET_COL_NAME]), test.select(*[FEATURE_COLS_NAME, ENCODED_TARGET_COL_NAME])

            train.write.csv(self.data_transformation_config.train_file_path)

            test.write.csv(self.data_transformation_config.test_file_path)

            preprocessor.save(self.data_transformation_config.pipeline_file_path)

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

