#!/usr/local/bin/python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from urllib.request import Request, urlopen
from pyspark.sql.types import *
import boto3
import configparser
import datetime
import json
import jsonschema
from jsonschema import validate

parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
api_key = parser.get("openweathermap", "api-key")

url="http://api.openweathermap.org/data/2.5/weather?q=Spain&appid=" + api_key

context = SparkContext(master="local[*]", appName="readJSON")
spark = SparkSession.builder.getOrCreate()

schema = StructType([
        StructField("main", StructType([
            StructField("temp", FloatType()),
            StructField("feels_like", FloatType()),
            StructField("temp_min", FloatType()),
            StructField("temp_max", FloatType()),
            StructField("pressure", FloatType()),
            StructField("humidity", FloatType())
        ])),
        StructField("id", IntegerType()),
        StructField("name", StringType())
])

# read json api
httpData = urlopen(url).read().decode('utf-8')
print(httpData)

# convert to dataframe with an imposed schema to make sure that the structure is correct. We might do this as well with json-schemas (in json format)
rdd = context.parallelize([httpData])
jsonDF = spark.read.json(rdd, schema=schema)
jsonDF.printSchema()
# root
#  |-- main: struct (nullable = true)
#  |    |-- temp: float (nullable = true)
#  |    |-- feels_like: float (nullable = true)
#  |    |-- temp_min: float (nullable = true)
#  |    |-- temp_max: float (nullable = true)
#  |    |-- pressure: float (nullable = true)
#  |    |-- humidity: float (nullable = true)
#  |-- id: integer (nullable = true)
#  |-- name: string (nullable = true)

jsonDF.show()
# +--------------------+-------+-----+
# |                main|     id| name|
# +--------------------+-------+-----+
# |{282.57, 280.01, ...|2510769|Spain|
# +--------------------+-------+-----+

inJson = jsonDF.toJSON().first()
print(inJson)
# {"main":{"temp":283.38,"feels_like":282.6,"temp_min":282.45,"temp_max":284.31,"pressure":1016.0,"humidity":82.0},"id":2510769,"name":"Spain"}

# write json into s3
s3 = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key = secret_key
)


now = datetime.datetime.now()
s3object = s3.Object(bucket_name, f'{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.json')
s3object.put(
    Body=(bytes(inJson.encode('UTF-8')))
)

