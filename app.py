import pyspark
from pyspark.sql import SparkSession
import config

class App:
    def __init__(self):
        spark = SparkSession.builder.appName('SCPTii').getOrCreate()
        self.df = spark.read.options(header='True', inferSchema='True').csv(config.hdfs_url+config.hdfs_path)

    def problem1(self):
        self.df.groupBy('classname', 'day', 'month', 'year').count().show()
    
