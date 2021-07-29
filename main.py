import app
import get_csv
import pydoop.hdfs as hdfs
import pydoop
from pyspark.sql import SparkSession


# Initiate pydoop connection to HDFS server
pydoop.hdfs.hdfs(host='192.168.56.200', port=9870, user='bdm')

spark = SparkSession.builder.appName('SCPTii').getOrCreate()

get_csv.GetCSV(spark)

# solution = app.App(spark)

# solution.problem1()
