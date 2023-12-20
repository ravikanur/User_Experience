import os
from pyspark.sql import SparkSession

#spark_session = SparkSession.builder.master('local[*]').appName('User_exp').config("spark.jars", "mysql-connector-java-8.0.13.jar").config("spark.driver.extraClassPath", "mysql-connector-java-8.0.13.jar").getOrCreate()
spark_session = SparkSession.builder.appName('User_exp').config("spark.jars", "mysql-connector-java-8.0.13.jar").master('local[*]').getOrCreate()

