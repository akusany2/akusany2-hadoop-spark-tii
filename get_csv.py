import requests
import shutil
import os
import pydoop.hdfs as hdfs
import config
import pandas as pd
import ssl


class GetCSV:

    def __init__(self, spark):
        # Uncomment the below function, if we want to clean the HDFS and re-download the files - useful if the data gets corrupted for some reason
        # self.clean_csv()

        # Looping through the URLs for both the weeks
        for i in range(0, 7):
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

    # Check if the HDFS directory exists (/tii-data) and local directory (./data) to keep the downloaded file temporarily
    def check_files(self, name):
        if not hdfs.path.isdir(config.hdfs_path):
            hdfs.mkdir(config.hdfs_path)

        if not os.path.isdir('./data'):
            os.mkdir('./data')
            
        return hdfs.path.isfile(config.hdfs_path+"/{}.csv".format(name))


    # Gets called from inside the loop and accepts URL and file name.
    # Used to download the file and upload to HDFS
    def get_csv(self, url, name):
        # If the file already exists in HDFS then skip download
        if not self.check_files(name):
            ssl._create_default_https_context = ssl._create_unverified_context

            file_name = name + ".csv"
            file_path = "./data/" + file_name

            # Passing the header on each request made to data.tii.ie to resemble browser request and not as bot
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
            }
            if not os.path.isfile(file_path):
                print("Downloading: {}".format(name))
                
                with requests.get(url, stream=True) as r:
                    with open(file_path, 'wb') as f:
                        shutil.copyfileobj(r.raw, f)

            print("Copying to HDFS: {}".format(name))

            # Uplaoding files to HDFS
            hdfs.put(file_path, config.hdfs_url+config.hdfs_path)
            # Deleting the files form temporary directoty (./data), so that it wont take more extra space
            os.remove(file_path)

        else:
            print("File {} already exists".format(name))
    
    def clean_csv(self):
        hdfs.rm(config.hdfs_path)
