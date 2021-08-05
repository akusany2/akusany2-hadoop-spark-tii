import app
import get_csv
import pydoop.hdfs as hdfs
import pydoop
from pyspark.sql import SparkSession

import csv


# Initiate pydoop connection to HDFS server
pydoop.hdfs.hdfs(host='ec2-34-229-205-101.compute-1.amazonaws.com', port=9870, user='ubuntu')

# SparkSession is entry point of SparkSQL
spark = SparkSession.builder.appName('SCPTii').getOrCreate()

# Calling function to populate to download the files from data.tii.ie and upload to HDFS
get_csv.GetCSV(spark)

# Creating an instance for SQL queries, the methods are defined as problem1, problem2 and so on for 5 problems defined in the document
solution = app.App(spark)

solution.problem1()
solution.problem2()
solution.problem3()
solution.problem4()
solution.problem5()

