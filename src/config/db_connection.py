import sys
from datetime import datetime
import pymysql

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, count, when, isnan, col

from src.config.spark_manager import spark_session
from src.constants.training_pipeline import *
from src.constants.prediction_pipeline import *
from src.entity.config_entity import TrainingPipelineConfig

from src.logger import logging
from src.exception import UserException

class MysqlConnection:
    def __init__(self):
        self.host = 'database-1.crg02eic2yup.ap-south-1.rds.amazonaws.com'
        self.port = 3306
        self.user = 'admin'
        self.passwd = '9oAKmVX3ClGlM2EYP4Cb'
        self.db = 'User_Experience'

    def connect_mysql(self):
        return pymysql.Connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db)

def insert_data_db(data: DataFrame, table_name: str, field_mapping: dict, 
                        null_replace_value: dict = None) -> None:
    try:
        logging.info("Entered insert_train_data_db method")
        sql_con = MysqlConnection()

        con = sql_con.connect_mysql()

        cursor = con.cursor()

        data = data.withColumn('submit_date', current_timestamp())

        null_val_per_col_count = data.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data.columns if c != 'submit_date']).collect()

        null_val_per_col_count_dict = null_val_per_col_count[0].asDict()

        null_val_col_df = [key for key, val in null_val_per_col_count_dict.items() if val != 0]

        null_val_col_db = []

        if len(null_val_col_df) > 0:
            null_val_col_db = [field_mapping[x] for x in null_val_col_df]
            if null_replace_value is not None:
                data.na.fill(null_replace_value)
            else:
                data.na.fill("")

        fields = [field_mapping[x] for x in data.columns]

        fields = ", ".join(fields)

        field_format = ["%s" for i in range(len(data.columns))]

        field_format = ", ".join(field_format)

        sql_query = f"INSERT INTO {table_name} ({fields}) VALUES ({field_format})"

        values = []

        rows = data.collect()
        logging.info(f"No of rows that will be inserted into db is {len(rows)}")
        for row in rows:
            row_list = tuple(row.asDict().values())

            values.append(row_list)
        
        logging.info(f"Count of values {len(values)}")
        logging.info("running the sql query")
        cursor.executemany(sql_query, values)
        logging.info("Committing the query")
        con.commit()
        logging.info("Successfully inserted data inot DB")
    except Exception as e:
        logging.error(e)
        raise UserException(e, sys)

def load_data_db(table_name, sql_query=None):
    try:
        logging.info("Entered load_data_db method")
        if sql_query is None:
            sql_query = table_name
        else:
            sql_query = sql_query

        user_df = spark_session.read.format("jdbc")\
                                    .option("driver", "com.mysql.cj.jdbc.Driver")\
                                    .option("url", "jdbc:mysql://database-1.crg02eic2yup.ap-south-1.rds.amazonaws.com/User_Experience?useSSL=FALSE&nullCatalogMeansCurrent=true&zeroDateTimeBehavior=convertToNull")\
                                    .option("dbtable", sql_query)\
                                    .option("user", "admin")\
                                    .option("password", "9oAKmVX3ClGlM2EYP4Cb")\
                                    .load()

        #print(user_df.show())
        if user_df.count() == 0:
            return None

        '''user_df.createOrReplaceTempView(table_name)

        if sql_query is None:
            user_df1 = sql(f"select * from {table_name}")
        else:
            user_df1 = sql(sql_query)'''
        
        logging.info(f"loaded data from db. Count is {user_df.count()}")
        return user_df

    except Exception as e:
        logging.error(e)
        raise UserException(e, sys)

if __name__ == '__main__':
    tp = TrainingPipelineConfig()
    user_df = spark_session.read.parquet('./user_exp_artifact/data_validation/User_final_data.parquet*')
    logging.info(f"No of rows is {user_df.count()}")
    user_df = user_df.drop(*COLS_TO_BE_REMOVED_DB)
    db_train_mapping = tp.config['db_mapping_train']
    insert_data_db(user_df, TRAINING_DB_TABLE_NAME, db_train_mapping)
    #df = load_data_db("train_data")
    #df.write.mode('overwrite').csv('./output.csv', header=True)