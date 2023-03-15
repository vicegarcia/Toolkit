import json
import os
from decouple import config


class Domain:
    def __init__(self, chose):
        self.domain = chose

    def multiple_files(self):
        files = os.listdir(
            config('PATH_INPUT')
        )
        for file in files:
            self.upload_file(
                config('PATH_INPUT') + file
            )

    def upload_file(self, path):
        with open(path, 'r') as file_open:
            data = json.load(file_open)
            self.domain_change(data)

    def domain_change(self, data):
        domain_list: dict = {
            'dev':['sause2tcccudpdev37', 'jesusagutierrez@coca-cola.com', 'jesusagutierrez@coca-cola.com'],
            'uat':['sause2tcccudpuat08', 'ricardgonzalez@coca-cola.com', 'Ricardo Gonzalez'],
            'prod':['sause2tcccudpprod09', '', '']
        }
        # data['sourceowners']['alias'] = domain_list[self.domain][]