import json
import os
from decouple import config

file = "spectra_Nielsen Spectra Retail Extract_reduced"

rute = config('PATH_ROOT')
archive = os.path.abspath(f'{rute}input/{file}.json')
template = os.path.abspath(f'{rute}/template/temp2.json')
output = os.path.abspath(f'{rute}/output/')

types = {"StringType": "String", "IntegerType": "Int", "TimestampType": "Timestamp",
         "ByteType": "String", "DecimalType": "Decimal", "FloatType": "Float", "DecimalType(38,18)": "Decimal",
         "DoubleType": "Double", "LongType": "Long"}
rare = ["ByteType", "TimestampType", "DateType"]

with open(template, 'r') as file_open:
    temp = json.load(file_open)

with open(archive, 'r') as file_open2:
    cf = json.load(file_open2)

temp["tenantshortname"] = cf["tenant"].lower()
temp["datasetname"] = cf["dataset"].lower()
temp["source"] = "naoumaster"
temp["edlconnection"]["storageaccount"] = config('STORE_ACCOUNT')
temp["domain"] = cf["entity"]["domain"].lower()

temp["sourceowners"] = [{"alias": config('SOURCEOWNER_ALIAS'), "displayname": config("SOURCEOWNER_DISPLAYNAME")}]
temp["notificationowners"] = [{"alias": config('NOOWNER_ALIAS'), "displayname": config('NOOWNER_DISPLAY')}]
temp["metadataowners"] = [{"alias": config('METAOWNER_ALIAS'), "displayname": config('METAOWNER_DISPLAY')}]


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

# Add columns

for column in cf["columns"]:
    new_col = {
        "SourceColumnName".lower(): column["source_column_name"],
        "SourceValueType".lower(): types[column["source_value_type"]],
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

    if new_col["valuetype"] in rare:
        new_col["valuetype"] = "StringType"
        new_col["sourcevaluetype"] = "String"

name_file: str = f"{temp['entity'].capitalize()}-{temp['datasetname'].capitalize()}-{temp['source'].capitalize()}-Full"

# Temporal Changes
temp["tenantshortname"] = "naoumaster"
temp["source"] = "naoumaster"
temp["edlconnection"]["storageaccount"] = "sause2tcccudpdev37"
temp["fileproperties"]["sourcerootpath"] = f'{temp["entity"]}/'
temp["datasetname"] = "product"
temp["ingestionpattern"] = "pull"

# Ingestion Source Connection

temp["ingestionsourceconnection"]["type"] = "regions"
temp["ingestionsourceconnection"]["accountname"] = "sause2tcccudpdev37"
temp["ingestionsourceconnection"]["connectionobject"]["accesskey"] = 'naoukey'
temp["ingestionsourceconnection"]["containername"] = "spool"

temp["fileproperties"]["tenantfileextension"] = "parquet"
# temp["ingestionsourceconnection"]
# temp["ingestionsourceconnection"]
# temp["ingestionsourceconnection"]
# temp["ingestionsourceconnection"]

temp["fileproperties"]["sourcerootpath"] = cf["file_properties"]["full_source_root_path"]

with open(os.path.join(output, f"{name_file}.json"), "w") as new_file:
    json.dump(temp, new_file, indent=2)
    print(f"The file {name_file} has been created")
