import pyspark
import pyspark.sql
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
import config

class App:
    # Defining schema for tii dataset CSV
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

        # Defining table view, to be used in SQL queries
        self.df.registerTempTable("tii_table")

    # Calculate the usage of Irish road network in terms of daily count average grouped by vehicle category. Report these percentages for each week separately.
    def problem1(self):
        self.spark.sql("""SELECT c.classname, CONCAT(CAST(c.day AS string), '-', CAST(c.month AS string), '-', CAST(c.year AS string)) AS date, 
        (c.per_class_count/c.per_day_class_count) * 100 AS average 
        FROM
        (SELECT classname, COUNT(classname) AS per_class_count, 
        SUM(COUNT(classname)) OVER (PARTITION BY day, year) AS per_day_class_count,
        day, month, year 
        FROM tii_table 
        WHERE classname IS NOT null
        GROUP BY day, month, year, classname) AS c 
        GROUP BY c.per_class_count, c.per_day_class_count, c.classname, c.day, c.month, c.year
        ORDER BY c.day, c.year DESC;""").show(100)

    
    # Identify the highest daily count site - show the date and total number of vehicles COUNT.
    def problem2(self):
        self.spark.sql("""SELECT c.cosit, MAX(c.cosit_count) AS total_vehicles, 
        CONCAT(CAST(c.day AS string), '-', CAST(c.month AS string), '-', CAST(c.year AS string)) AS date 
        FROM (SELECT cosit, COUNT(cosit) AS cosit_count, day, month, year FROM tii_table GROUP BY cosit, day, month, year 
        ORDER BY cosit_count DESC) AS c 
        GROUP BY c.cosit, c.day, c.month, c.year ORDER BY total_vehicles DESC LIMIT 1;""").show()

    # Which sites have seen a significant amount of traffic reduction due to lockdown (list top 10)?
    def problem3(self):
        self.spark.sql("""SELECT w1.cosit, abs(((w1.cosit_count - w2.cosit_count) /w1.cosit_count)*100) AS traffic_diff_percent FROM (SELECT cosit, COUNT(cosit) AS cosit_count FROM tii_table WHERE year=2019 GROUP BY cosit) AS w1 
        LEFT JOIN 
        (SELECT cosit, COUNT(cosit) AS cosit_count FROM tii_table WHERE year=2020 GROUP BY cosit) AS w2 
        ON (w1.cosit = w2.cosit) ORDER BY traffic_diff_percent DESC LIMIT 10;""").show()

    # Calculate average daily speed for both weeks. Do you observe a difference in driving behaviour?
    def problem4(self):
        self.spark.sql("""SELECT avg(speed) AS average_speed,
        CONCAT(CAST(day AS string), '-', CAST(month AS string), '-', CAST(year AS string)) AS date 
        FROM tii_table GROUP BY day, month, year;""").show()

    # Calculate the top 10 locations with highest number of count of HGVs (class) for both weeks
    def problem5(self):
        self.spark.sql('SELECT cosit, COUNT(cosit) AS total_HGVs FROM tii_table WHERE classname LIKE "HGV%" GROUP BY cosit ORDER BY total_HGVs DESC LIMIT 10;').show()
