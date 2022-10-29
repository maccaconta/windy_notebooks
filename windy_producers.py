# Databricks notebook source
#!pip install requests

# COMMAND ----------

import requests
import json

# COMMAND ----------

url_ = 'https://api.windy.com/api/point-forecast/v2'
pay_ = {
    'lat': 49.809,
    'lon': 16.787,
    'model': 'gfs',
    'parameters': ['wind', 'precip','temp'],
    'levels': ['surface', '300h'],
    'key': 'rfQcTwC7FjCCRRF692GssCavS0LhO4EF'
}

retorno_ = requests.post(url_, json = pay_, headers = {'content-type' : 'application/json'}).json()

print(retorno_) 


# COMMAND ----------

dumps_ = json.dumps(retorno_)

# COMMAND ----------

dbutils.fs.put('/temp/windy.json',dumps_,True)

# COMMAND ----------

dbutils.fs.ls('/temp/windy.json')

# COMMAND ----------

df = spark.read.json('/temp/windy.json')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()
