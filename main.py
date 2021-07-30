import app
import get_csv
import pydoop.hdfs as hdfs
import pydoop
from pyspark.sql import SparkSession

import csv


# Initiate pydoop connection to HDFS server
pydoop.hdfs.hdfs(host='192.168.56.200', port=9870, user='bdm')

spark = SparkSession.builder.appName('SCPTii').getOrCreate()

# conf = SparkConf().setAppName('SCPTii').setMaster('yarn')
# spark = SparkContext(conf=conf)

# get_csv.GetCSV(spark)

solution = app.App(spark)

solution.problem1()

# count = 0
# with open('data/per-vehicle-records-2019-04-8.csv', newline='') as csvfile:
#     spamreader = csv.reader(csvfile, delimiter=' ')
#     for row in spamreader:
#         # print(row)
#         for i in row:
#             if i == "BUS":
#                 count += 1
    
# print(count)