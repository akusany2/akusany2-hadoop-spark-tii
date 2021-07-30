import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import config

class App:
    tii_schema = StructType([
        StructField("cosit",StringType(),True),
        StructField("year",IntegerType(),True),
        StructField("month",IntegerType(),True),
        StructField("day",IntegerType(),True),
        StructField("hour",IntegerType(),True),
        StructField("minute",IntegerType(),True),
        StructField("second",IntegerType(),True),
        StructField("millisecond",IntegerType(),True),
        StructField("minuteofday",IntegerType(),True),
        StructField("lane",IntegerType(),True),
        StructField("lanename", StringType(), True),
        StructField("straddlelane",IntegerType(),True),
        StructField("straddlelanename", StringType(), True),
        StructField("class",IntegerType(),True),
        StructField("classname", StringType(), True),
        StructField("length",IntegerType(),True),
        StructField("headway",IntegerType(),True),
        StructField("gap",IntegerType(),True),
        StructField("speed",IntegerType(),True)
    ])
    def __init__(self, spark):
        self.df = spark.read.options(header='True', inferSchema='True').schema(self.tii_schema).csv(config.hdfs_url+config.hdfs_path)

    # Calculate the usage of Irish road network in terms of daily count average grouped by vehicle category. Report these percentages for each week separately.
    def problem1(self):
        self.df.groupBy('classname', 'day', 'month', 'year').count().sort('year').show()
    
    # Identify the highest daily count site - show the date and total number of vehicles counts.
    def problem2(self):
        self.df.groupBy('classname', 'day', 'month', 'year').count().sort('year').show()
