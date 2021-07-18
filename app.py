import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SCPTii').getOrCreate()
df = spark.read.options(header='True', inferSchema='True').csv("./data")

df.groupBy('classname', 'day', 'month', 'year').count().show()