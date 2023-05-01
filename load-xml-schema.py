# Databricks notebook source

tar_gz_filepath = "/FileStore/zip-xml/data_tar.gz"
xml_filepath = "/FileStore/zip-xml/data.xml"


# COMMAND ----------

# MAGIC %fs ls /FileStore/zip-xml/

# COMMAND ----------

df = spark.read.format("xml").option("rootTag", "Invoices").option("rowTag", "Invoice").load(xml_filepath)

# load the file using spark.read
#df = spark.read.format("xml").option("rowTag", "your_row_tag").load(f"tar://{filepath}!file.xml")

# show the data
df.show()

# COMMAND ----------

dfb = spark.read.format("binaryFile").load(tar_gz_filepath)

dfb.show()

# COMMAND ----------

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

from pyspark.sql.functions import col, hex
df = dfb.withColumn("json", toStrUDF(dfb.content)).drop("content","length")
df.createOrReplaceTempView("json_data")

# COMMAND ----------

# MAGIC %sql select * from json_data

# COMMAND ----------

from pyspark.sql.functions import from_json

schema = spark.read.json(df.select("json").rdd.map(lambda x: x[0])).schema
schema.json()
