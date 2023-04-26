import json
import os
from decouple import config


class Downgrade:
    """ This Class transform the control files from V2 To  V1
        Create a folder input and output or change the paths in .env file
        Use the open_folder( ) method !!!.
    """
    def __init__(self):
        self.rute = config('PATH_ROOT')
        self.output = config('PATH_OUTPUT')
        self.types: dict = {
            "StringType": "String", "IntegerType": "Int", "TimestampType": "Timestamp",
            "ByteType": "String", "DecimalType": "Decimal", "FloatType": "Float", "DecimalType(38,18)": "Decimal",
            "DoubleType": "Double", "LongType": "Long"}
        self.rare: list = [
            "ByteType", "TimestampType", "DateType"]

    def open_folder(self):
        files = os.listdir(config('PATH_INPUT'))
        for file in files:
            self.open_files(config('PATH_INPUT') + file)

    def open_files(self, path_file):
        template = config('PATH_TEMPLATE')
        path = path_file
        self.creation_dictionaries(template, path)

    def creation_dictionaries(self, template, path):
        with open(template, 'r') as template_open:
            temp = json.load(template_open)
        with open(path, 'r') as file_open:
            cf = json.load(file_open)
        self.change_values(temp, cf)

    def change_values(self, temp, cf):
        temp["tenantshortname"] = cf["tenant"].lower()
        temp["datasetname"] = cf["dataset"].lower()
        temp["source"] = "naoumaster"
        temp["edlconnection"]["storageaccount"] = config('STORE_ACCOUNT')
        temp["domain"] = cf["entity"]["domain"].lower()
        # OWNERS
        temp["sourceowners"] = [
            {"alias": config('SOURCEOWNER_ALIAS'), "displayname": config("SOURCEOWNER_DISPLAYNAME")}]
        temp["notificationowners"] = [{"alias": config('NOOWNER_ALIAS'), "displayname": config('NOOWNER_DISPLAY')}]
        temp["metadataowners"] = [{"alias": config('METAOWNER_ALIAS'), "displayname": config('METAOWNER_DISPLAY')}]
        # BUSINESS CHANGES
        temp["entity"] = cf["entity_name"].lower().replace(" ", "_").replace("&", "and")
        temp["entitysourcename"] = cf["entity_name"].lower()
        temp["fullyqualifiedname"] = cf["entity_name"].lower()
        temp["businessname"] = cf["entity"]["businessname"].lower()
        temp["description"] = cf["entity"]["entity_description"]
        temp["version"] = cf["entity"]["entity_version"]
        temp["typeofdata"] = cf["entity"]["type_of_data"]
        temp["ingestionpattern"] = cf["entity"]["ingestion_pattern"]
        temp["fileproperties"]["sourcerootpath"] = cf["file_properties"]["full_source_root_path"]
        temp["fileproperties"]["sourcefilename"] = ""
        temp["ingestionsourceconnection"]["accountname"] = config('STORE_ACCOUNT')
        self.add_columns(temp, cf)

    def add_columns(self, temp, cf):
        for column in cf["columns"]:
            new_col = {
                "SourceColumnName".lower(): column["source_column_name"],
                "SourceValueType".lower(): self.types[column["source_value_type"]],
                "Description".lower(): column["description"],
                "OverrideExpression".lower(): None,
                "allowEmpty".lower(): "false",
                "name".lower(): column["source_column_name"],
                "ordinal".lower(): column["ordinal"],
                "valueType".lower(): column["source_value_type"],
                "nullable".lower(): column["nullable"],
                "format".lower(): column["format"],
                "size".lower(): 255,
                "precision".lower(): column["precision"],
                "primaryKey".lower(): column["primarykey"],
                "sensitive".lower(): column["sensitive"],
                "masked".lower(): column["masked"],
                "encrypted".lower(): column["encrypted"],
                "authFactoryType".lower(): "none",
                "subjectDetails".lower(): None,
                "status".lower(): "active"
            }
            temp["columns"].append(new_col)
            # CHANGE TYPES FOR STRING
            if new_col["valuetype"] in self.rare:
                new_col["valuetype"] = "StringType"
                new_col["sourcevaluetype"] = "String"
        self.temporal_changes(temp, cf)

    def temporal_changes(self, temp, cf):
        # CHANGES FOR NAOU'S ENTITIES
        temp["tenantshortname"] = "naoumaster"
        temp["source"] = "naoumaster"
        temp["edlconnection"]["storageaccount"] = "sause2tcccudpdev37"
        temp["fileproperties"]["sourcerootpath"] = f'{temp["entity"]}/'
        temp["datasetname"] = "product"
        temp["ingestionpattern"] = "pull"
        # CONNECTION RESOURCES
        temp["ingestionsourceconnection"]["type"] = "regions"
        temp["ingestionsourceconnection"]["accountname"] = "sause2tcccudpdev37"
        temp["ingestionsourceconnection"]["connectionobject"]["accesskey"] = 'naoukey'
        temp["ingestionsourceconnection"]["containername"] = "spool"
        # FILES PATH
        temp["fileproperties"]["tenantfileextension"] = "parquet"
        temp["fileproperties"]["sourcerootpath"] = cf["file_properties"]["full_source_root_path"]
        self.save_new_cf(temp)

    def save_new_cf(self, temp):
        name_file: str =\
            f"{temp['entity'].capitalize()}-{temp['datasetname'].capitalize()}-{temp['source'].capitalize()}-Full"
        with open(os.path.join(self.output, f"{name_file}.json"), "w") as new_file:
            json.dump(temp, new_file, indent=2)
            print(f"The file {name_file} has been created")



