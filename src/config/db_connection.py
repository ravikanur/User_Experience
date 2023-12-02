import sys
from datetime import datetime
import pymysql

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, count, when, isnan, col

from src.config.spark_manager import spark_session
from src.constants.training_pipeline import *
from src.constants.prediction_pipeline import *

from src.logger import logging
from src.exception import UserException

class MysqlConnection:
    def __init__(self):
        self.host = 'database-1.c9o5ipc0qopt.ap-south-1.rds.amazonaws.com'
        self.port = 3306
        self.user = 'admin'
        self.passwd = 'wfaZh92h'
        self.db = 'sensor_temp'

    def connect_mysql(self):
        return pymysql.Connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db)

def insert_train_data_db(data: DataFrame, table_name: str, field_mapping: dict, 
                        null_replace_value: dict = None) -> None:
    try:
        logging.info("Entered insert_train_data_db method")
        con = MysqlConnection()

        sql_con = con.connect_mysql()

        cursor = sql_con.cursor()

        data = data.withColumn('submit_date', current_timestamp())

        null_val_per_col_count = data.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data.columns]).collect()

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

        for row in rows:
            row_list = tuple(row.asDict().values())

            values.append(row_list)
        
        cursor.executemany(sql_query, values)

        con.commit()
    except Exception as e:
        logging.error(e)
        raise UserException(e, sys)