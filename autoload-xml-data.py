# Databricks notebook source
# MAGIC %pip install xmltodict

# COMMAND ----------

import tarfile, io
# import pandas as pd

import xmltodict, json

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# define a UDF to convert binary tar.gz files to text
def binary_tar_to_text(binary_data):
    # convert binary data to BytesIO object
    bytes_io = io.BytesIO(binary_data)

    # extract tar archive
    with tarfile.open(fileobj=bytes_io, mode='r:gz') as tar_archive:
        # assume the archive contains only one file
        filename = tar_archive.getnames()[0]
        file_content = tar_archive.extractfile(filename).read()

    # convert binary content to text
    text_content = file_content.decode('utf-8')
    # df = pd.read_xml(text_content)

    # df_json = df.to_json()

    obj = xmltodict.parse(text_content)
    dict_json = json.dumps(obj)

    return dict_json

toStrUDF = udf(lambda bytes: binary_tar_to_text(bytes), StringType())

# COMMAND ----------

from pyspark.sql.types import StructType    

# Save schema from the original DataFrame into json:
schema_json = '{"fields":[{"metadata":{},"name":"Invoices","nullable":true,"type":{"fields":[{"metadata":{},"name":"Invoice","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"@id","nullable":true,"type":"string"},{"metadata":{},"name":"Customer","nullable":true,"type":{"fields":[{"metadata":{},"name":"Address","nullable":true,"type":{"fields":[{"metadata":{},"name":"City","nullable":true,"type":"string"},{"metadata":{},"name":"State","nullable":true,"type":"string"},{"metadata":{},"name":"Street","nullable":true,"type":"string"},{"metadata":{},"name":"Zip","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"Name","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"InvoiceItems","nullable":true,"type":{"fields":[{"metadata":{},"name":"InvoiceItem","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"@id","nullable":true,"type":"string"},{"metadata":{},"name":"Description","nullable":true,"type":"string"},{"metadata":{},"name":"Price","nullable":true,"type":"string"},{"metadata":{},"name":"Quantity","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}}],"type":"struct"}}],"type":"struct"},"type":"array"}}],"type":"struct"}}],"type":"struct"}'

# Restore schema from json:
import json
json_schema = StructType.fromJson(json.loads(schema_json))

# COMMAND ----------

from pyspark.sql.functions import col,from_json, explode
tar_gz_filepath = "/FileStore/zip-xml/"

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "binaryFile") \
  .option("cloudFiles.schemaLocation", "/FileStore/zip-xml/schema") \
  .option("pathGlobfilter", "*.gz") \
  .load("/FileStore/zip-xml/") \
  .withColumn("json", toStrUDF(col("content"))) \
  .drop("content","length") \
  .withColumn("data",from_json(col("json"),json_schema)) \
  .select("path", "modificationTime", "data.*") \
  .select("path","modificationTime",explode("Invoices.Invoice").alias("invoice")) \
  .select("path","modificationTime","invoice.*") \
  .writeStream \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "/FileStore/zip-xml/checkpoint") \
  .trigger(availableNow=True) \
  .toTable("bjerde.playground.xml")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bjerde.playground.xml
