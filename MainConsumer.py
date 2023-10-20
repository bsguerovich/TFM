from pyspark.sql.types import *
import pyspark.sql.functions as F
from transformers import pipeline
from pyspark.sql.types import StringType, StructType, StructField
from google.cloud import bigquery
from google.oauth2 import service_account
from transformers import AutoTokenizer, AutoModelForTokenClassification
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf
import pandas as pd
from pyspark.sql.functions import regexp_extract
import json

# BIG QUERY
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "dbfs:/FileStore/Key/tfm-bigdata-2023-e1b1cd47ed9a-bq-storage.json")
table_full_path = "tfm-bigdata-2023.TFM.Noticias"
gcs_temp_bucket = "tfm-temporary-bucket"
checkpointLocation = "databricks-5627788356269344/5627788356269344/FileStore/checkpointLocation"

# AZURE EVENT HUB - AZURE CONECTION
connectionString = "Endpoint=sb://event-web-tfm.servicebus.windows.net/;SharedAccessKeyName=my-sender;SharedAccessKey=uhchQOtzjwfIHH9D8RkzKzlHcLAooyfte+AEhNb4V2I=;EntityPath=webscraping"
ehConf = {}
ehConf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf["eventhubs.consumerGroup"] = "$Default"


#pipelines
sentiment_pipeline = pipeline("text-classification", model="nlptown/bert-base-multilingual-uncased-sentiment")
language_pipeline = pipeline("text-classification", model="papluca/xlm-roberta-base-language-detection")

# UDF - CLASSIFY SENTIMENT
def classify_sentiment(text):
    return sentiment_pipeline(text)[0]['label']

# UDF - CLASSIFY LANGUAGE
def classify_language(text):
    return language_pipeline(text)[0]['label']

#Registrar la funcion en udf
sentiment_udf = udf(classify_sentiment, StringType())
language_udf = udf(classify_language, StringType())

# SCHEMA
json_schema = StructType(
    [
        StructField("Empresa", StringType(), True),
        StructField("Titular", StringType(), True),
        StructField("Link", StringType(), True)
    ]
)
df = spark.readStream.format("eventhubs").options(**ehConf).load()

json_df = df.withColumn("body", F.from_json(df.body.cast("string"), json_schema))

df3 = json_df.select(
    F.col("body.Empresa"),
    F.col("body.Titular"), 
    F.col("body.Link"),
    F.current_timestamp().alias("Fecha"))

df4 = df3.withColumn("Sentimiento", sentiment_udf(F.col("Titular"))) \
         .withColumn("Idioma", language_udf(F.col("Titular")))

#df_final = json_df.select(
#    F.col("body.Empresa"),
#    F.col("body.Titular"), 
#    F.col("body.Link"),
#    F.current_timestamp().alias("Fecha")
#).withColumn("Sentimiento", sentiment_udf(F.col("Titular"))) \
# .withColumn("Idioma", language_udf(F.col("Titular")))


df4.writeStream \
    .format("bigquery") \
    .option("table", table_full_path) \
    .option("checkpointLocation", checkpointLocation) \
    .option("temporaryGcsBucket", gcs_temp_bucket) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
#display(df4)