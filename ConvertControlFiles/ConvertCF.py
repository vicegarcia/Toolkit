import json
import os
from decouple import config


class DeltaToFull:
    def __init__(self):
        pass

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
            self.delete_values(data)

    def delete_values(self, data):
        delete: list = [
            'id', 'sourcerootpath', 'sourcefilename', 'encryptedcolumns', 'primarykeycolumns', 'keyvaultname',
            'keyvaultkeyname', 'breakpoint', 'partofstandarddatamodel', 'donefilerequired', 'purviewrootpath',
            '_rid', '_self', '_etag', '_attachments', '_ts']

        for x in delete:
            if x in data:
                del data[x]
        self.change_values(data)

    def change_values(self, data):
        data['loadtype'] = "full"
        data['moduleparameters'] = [[{"name": "load_type", "value": "full"}]]
        data["fileproperties"]["sourcefilename"] = ""

        slide = data["fileproperties"]["sourcerootpath"].split('/')
        if slide[-1] == "%y%M%d":
            slide.pop()
            slide[-1] = "v1"
            path = "/".join(slide)
            data["fileproperties"]["sourcerootpath"] = path
        self.set_name_file(data)

    def set_name_file(self, data):
        name_file: str = f"{data['entity'].capitalize()}-{data['datasetname'].capitalize()}-{data['source'][-3:]}-Full"
        self.save_as_delta(name_file, data)

    def save_as_delta(self, name_file, data):
        with open(config('PATH_OUTPUT') + name_file + ".json", "w") as new_file:
            json.dump(data, new_file, indent=2)
            print(f"The file {name_file} has been created")


class FullToDelta:
    def __init__(self):
        pass

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
            self.delete_filetype(data)

    def delete_filetype(self, data):
        try:
            del data["fileproperties"]["filetype"]
        except KeyError:
            self.delete_values(data)
        self.delete_values(data)

    def delete_values(self, data):
        delete: list = [
            'id', 'sourcerootpath', 'sourcefilename', 'encryptedcolumns', 'primarykeycolumns', 'keyvaultname',
            'keyvaultkeyname', 'breakpoint', 'partofstandarddatamodel', 'donefilerequired', 'purviewrootpath',
            '_rid', '_self', '_etag', '_attachments', '_ts']

        for x in delete:
            if x in data:
                del data[x]
        self.change_values(data)

    def change_values(self, data):
        data['loadtype'] = "delta"
        data['moduleparameters'] = []
        data["fileproperties"]["sourcefilename"] = ""
        slide = data["fileproperties"]["sourcerootpath"].split('/')
        if slide[-1] != "%y%M%d":
            slide[-1] = "%y%M%d"
            path = "/".join(slide)
            data["fileproperties"]["sourcerootpath"] = path

        self.set_name_file(data)

    def set_name_file(self, data):
        name_file: str = f"{data['entity'].capitalize()}-{data['datasetname'].capitalize()}-{data['source'][-3:]}-Delta"
        self.save_as_delta(name_file, data)

    def save_as_delta(self, name_file, data):
        with open(config('PATH_OUTPUT') + name_file + ".json", "w") as new_file:
            json.dump(data, new_file, indent=2)
            print(f"The file {name_file} has been created")


