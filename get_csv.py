import requests
import shutil
import os
import pydoop.hdfs as hdfs
import config
import pandas as pd
import ssl


class GetCSV:

    def __init__(self, spark):

        self.clean_csv()
        for i in range(0, 1):
            self.get_csv(
                "https://data.tii.ie/Datasets/TrafficCountData/2019/04/{date:02d}/per-vehicle-records-2019-04-{date:02d}.csv".format(
                    date=i + 8
                ),
                "per-vehicle-records-2019-04-{}".format(i + 8),
            )

            self.get_csv(
                "https://data.tii.ie/Datasets/TrafficCountData/2020/04/{date:02d}/per-vehicle-records-2020-04-{date:02d}.csv".format(
                    date=i + 6
                ),
                "per-vehicle-records-2020-04-{}".format(i + 6),
            )

    def check_files(self, name):
        if not hdfs.path.isdir(config.hdfs_path):
            hdfs.mkdir(config.hdfs_path)

        if not os.path.isdir('./data'):
            os.mkdir('./data')
            
        return hdfs.path.isfile(config.hdfs_path+"/{}.csv".format(name))


    def get_csv(self, url, name):
        if not self.check_files(name):
            ssl._create_default_https_context = ssl._create_unverified_context

            file_name = name + ".csv"
            file_path = "./data/" + file_name
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
            }
            if not os.path.isfile(file_path):
                print("Downloading: {}".format(name))
                
                with requests.get(url, stream=True) as r:
                    with open(file_path, 'wb') as f:
                        shutil.copyfileobj(r.raw, f)

            print("Copying to HDFS: {}".format(name))

            hdfs.put(file_path, config.hdfs_url+config.hdfs_path)

            os.remove(file_path)

        else:
            print("File {} already exists".format(name))
    
    def clean_csv(self):
        hdfs.rm(config.hdfs_path)
