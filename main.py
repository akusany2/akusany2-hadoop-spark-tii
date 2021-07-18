import app
import get_csv
import pydoop.hdfs as hdfs
import pydoop

# Initiate pydoop connection to HDFS server
pydoop.hdfs.hdfs(host='192.168.56.200', port=9870, user='bdm')


# get_csv.GetCSV()

solution = app.App()

solution.problem1()
