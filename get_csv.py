import requests
import os


class GetCSV:
    def __init__(self) -> None:
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

    def check_files(self, name):
        if not os.path.isdir('./data'):
            os.mkdir('./data')
            
        if os.path.isfile("./data/{}.csv".format(name)):
            return True
        else:
            return False

    def get_csv(self, url, name):
        if not self.check_files(name):

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
            }
            print("Downloading: {}".format(name))
            req = requests.get(url, headers=headers)
            url_content = req.content
            csv_file = open("./data/" + name + ".csv", "wb")

            csv_file.write(url_content)
            csv_file.close()

        else:
            print("File {} already exists".format(name))
