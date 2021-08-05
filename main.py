import app
import get_csv
import pydoop.hdfs as hdfs
import pydoop
from pyspark.sql import SparkSession

import csv


# Initiate pydoop connection to HDFS server
pydoop.hdfs.hdfs(host='ec2-34-229-205-101.compute-1.amazonaws.com', port=9870, user='ubuntu')

spark = SparkSession.builder.appName('SCPTii').getOrCreate()


get_csv.GetCSV(spark)

solution = app.App(spark)

solution.problem1()
solution.problem2()
solution.problem3()
solution.problem4()
solution.problem5()

