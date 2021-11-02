#!/usr/local/bin/python3s
import boto3
import configparser
import datetime
import json
import jsonschema
from jsonschema import validate
import requests

parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
api_key = parser.get("openweathermap", "api-key")

url="http://api.openweathermap.org/data/2.5/weather?q=Spain&appid=" + api_key

def get_schema():
    """This function loads the given schema available"""
    with open('api-json-schema.json', 'r') as file:
        schema = json.load(file)
    return schema

def validate_json(json_data):
    execute_api_schema = get_schema()

    try:
        validate(instance=json_data, schema=execute_api_schema)
    except jsonschema.exceptions.ValidationError as err:
        print(err)
        err = "Given JSON data is InValid"
        return False, err

    message = "Given JSON data is Valid"
    return True, message

# call api
inJson = requests.get(url).json()
print(inJson) # dict

# validate api response schema
is_valid, msg = validate_json(inJson)
print(msg)


# write json into s3
s3 = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key = secret_key
)


now = datetime.datetime.now()
s3object = s3.Object(bucket_name, f'{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.json')
s3object.put(
    Body=(bytes(json.dumps(inJson).encode('UTF-8')))
)

