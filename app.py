import pyspark
import pyspark.sql
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
import config

class App:
    tii_schema = StructType([
        StructField("cosit",IntegerType(),True),
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
        StructField("headway",FloatType(),True),
        StructField("gap",IntegerType(),True),
        StructField("speed",FloatType(),True)
    ])
    def __init__(self, spark):
        self.spark =  spark
        self.df = spark.read.options(header='True', inferSchema='True').schema(self.tii_schema).csv(config.hdfs_url+config.hdfs_path)
        self.df.registerTempTable("tii_table")

    # Calculate the usage of Irish road network in terms of daily count average grouped by vehicle category. Report these percentages for each week separately.
    def problem1(self):
        self.df.groupBy('classname', 'day', 'month', 'year').count().sort('year').show()
    
    # Identify the highest daily count site - show the date and total number of vehicles counts.
    def problem2(self):
        self.spark.sql("select count(cosit) as total_vehicles, day, month, year from tii_table group by day, month, year, cosit order by total_vehicles DESC LIMIT 1").show()

    # Which sites have seen a significant amount of traffic reduction due to lockdown (list top 10)?
    def problem3(self):
        self.spark.sql("select w1.cosit, (count(w1.cosit) - count(w2.cosit)/count(w1.cosit) * 100) as diff from (select cosit from tii_table where year=2019) as w1, (select cosit from tii_table where year=2020) as w2 group by w1.cosit;").show()

    # Calculate average daily speed for both weeks. Do you observe a difference in driving behaviour?
    def problem4(self):
        self.spark.sql("select avg(speed), day, month, year from tii_table group by day, month, year").show()

    # Calculate the top 10 locations with highest number of counts of HGVs (class) for both weeks
    def problem5(self):
        self.spark.sql("select cosit, count(cosit) as total_HGVs from tii_table where classname like 'HGV%' group by cosit order by total_HGVs DESC limit 10").show()
