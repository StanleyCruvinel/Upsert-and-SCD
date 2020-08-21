# Databricks notebook source
from pyspark.sql.functions import *
import json, requests
# pprint is used to format the JSON response
from pprint import pprint

# COMMAND ----------

# DBTITLE 1,Params
#Set Parameters
dbutils.widgets.removeAll()

ColumnName = dbutils.widgets.text("ColumnName","Comments")

#Get Parameters
ColumnName = dbutils.widgets.get("ColumnName")

#Print Date Range
print("ColumnName = {}".format(ColumnName))

# COMMAND ----------

# DBTITLE 1,Cognitive Services API - Config
key_var_name = '62bc476518ee45f9ada3510cdc5abef3'
subscription_key = key_var_name

endpoint_var_name = 'https://companyname-projectname-dev-textanalytics.cognitiveservices.azure.com'
endpoint = endpoint_var_name

language_api_url = endpoint + "/text/analytics/v3.0/Sentiment"
language_api_url

# COMMAND ----------

# DBTITLE 1,Import Dataframe
# File location and type
file_location = "/FileStore/tables/SentimentAnalysis.csv"

file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# DBTITLE 1,Add Unique ID, Friendly Columns
#Add Unique ID
df = df.withColumn("id", monotonically_increasing_id())
dfCog = df.filter(col(ColumnName).isNotNull())
dfCog = dfCog.selectExpr("id as ID", "Comments as Text").limit(10)
display(dfCog.limit(10))

# COMMAND ----------

# DBTITLE 1,Convert Dataframe To Json
dfCogJson = dfCog.toJSON()
dfCogJson = dfCogJson.collect()
resultlist_json = [json.loads(x) for x in dfCogJson] 
document = {"documents": resultlist_json}
pprint(document)

# COMMAND ----------

# DBTITLE 1,Congnitive Services - Send Request
headers = {"Ocp-Apim-Subscription-Key": subscription_key}
response = requests.post(language_api_url, headers=headers, json=document)
response = response.json()
response

# COMMAND ----------

# DBTITLE 1,Parallelize Json
dfCog = spark.read.json(sc.parallelize(response["documents"]), prefersDecimal=True, dateFormat="yyyy-MM-dd",timestampFormat="yyyy-MM-dd HH:mm:ss",multiLine=False)
dfCog = dfCog.filter(col("_corrupt_record").isNull())
display(dfCog)

# COMMAND ----------

# DBTITLE 1,Flatten Json
dfCog = dfCog.select(col("id").alias("Id"), 
                     col("sentiment").alias("Sentiment"),
                     explode(array("confidenceScores.positive")).alias("Positive"),
                    col("confidenceScores.neutral").alias("Neutral"),
        col("confidenceScores.negative").alias("Negative")
                 )

dfCog = dfCog.select("Id", "Sentiment", "Positive", "Neutral", "Negative")
display(dfCog)

# COMMAND ----------

# DBTITLE 1,Join Original Dataset - Drop Mon Id Column
dfFinal = df.join(dfCog, df.id == dfCog.Id, how="left").drop('Id')

# COMMAND ----------

# DBTITLE 1,Display Final Dataframe
display(dfFinal)
