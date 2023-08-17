import os
from pyspark.sql import SparkSession

spark_session = SparkSession.builder.master('local[*]').appName('User_exp').getOrCreate()

