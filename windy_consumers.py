# Databricks notebook source
# MAGIC %run /Shared/connection

# COMMAND ----------

spark.sparkContext.setLogLevel("INFO")

# COMMAND ----------

area_negocio="windy"
system_database="windy_hst"

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import arrays_zip, col, explode
from pyspark.sql.functions import col, explode, regexp_replace, split
from pyspark.sql.functions  import date_format

# COMMAND ----------

dbutils.fs.ls('/temp/windy.json')

# COMMAND ----------

df = spark.read.json('/temp/windy.json')
df.display()

# COMMAND ----------

def explode_cols( data, cols):
    data = data.withColumn('exp_combo', F.arrays_zip(*cols))
    data = data.withColumn('exp_combo', F.explode('exp_combo'))
    for col in cols:
        data = data.withColumn(col, F.col('exp_combo.' + col))
        
    return data.drop(F.col('exp_combo'))

# COMMAND ----------

cols = ["ts", "past3hprecip-surface","wind_u-surface"]
df_output = explode_cols(df,cols)
df_final = df_output.select(cols)
display(df_final)


# COMMAND ----------

from pyspark.sql.functions import udf    
import time

def epoch_to_datetime(x):
    return time.localtime(x)

# COMMAND ----------


df_transfor = df_final\
.withColumn("datetime", F.to_timestamp(df_final['ts']/1000))\
.withColumnRenamed("past3hprecip-surface","precipitacao")\
.withColumnRenamed("wind_u-surface","wind_u_surface")\
.withColumn("anomes", date_format(col("datetime"),"yyyyMM"))


df_transfor.createOrReplaceTempView("tmp_windy")
df_transfor.display()

# COMMAND ----------

spark.sql(f"""
drop table if exists stt_windy
""")

spark.sql(f"""

create table if not exists stt_windy (
   anomes              string,
   datetime            string,
   vento_u_superficie  float,
   precipitacao        float
)

USING DELTA
PARTITIONED BY (anomes)
LOCATION '{structured_storage_mount_path}/corporate/{area_negocio}/{system_database}/stt_windy'
""")

# COMMAND ----------

spark.sql(f'''
    insert into  stt_windy
    select
       anomes,
       datetime,
       wind_u_surface,
       precipitacao
    from
        tmp_windy
''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from stt_windy
