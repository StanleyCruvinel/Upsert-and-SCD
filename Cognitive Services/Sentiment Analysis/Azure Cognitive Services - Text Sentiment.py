# Databricks notebook source
from pyspark.sql.functions import *
import json, requests
# pprint is used to format the JSON response
from pprint import pprint

# COMMAND ----------

# DBTITLE 1,Import Dataframe
# File location and type
file_location = "/FileStore/tables/TwitterCovidTweets.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# DBTITLE 1,Params
#Set Parameters
dbutils.widgets.removeAll()

ColumnName = dbutils.widgets.text("ColumnName","")

#Get Parameters
ColumnName = dbutils.widgets.get("ColumnName")

#Print Date Range
print("ColumnName = {}".format(ColumnName))

# COMMAND ----------

# DBTITLE 1,Cognitive Services API - Config
endpoint = 'https://company-name-textanalytics.cognitiveservices.azure.com/text/analytics/v3.0/Sentiment'

subscription_key = 'c570a1c3f6784ceb9b9fcdfa8d6d317e'

# COMMAND ----------

# DBTITLE 1,Add Unique ID, Friendly Columns
#Create Select List
selectCols = ["ID"]
selectCols.append(ColumnName)

#Add Unique ID
df = df.withColumn("ID", monotonically_increasing_id())
dfCog = df.filter(col(ColumnName).isNotNull())
dfCog = dfCog.selectExpr(selectCols)
display(dfCog)

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
response = requests.post(endpoint, headers=headers, json=document)
response = response.json()
response

# COMMAND ----------

# DBTITLE 1,Parallelize Json
dfCog = spark.read.json(sc.parallelize(response["documents"]), prefersDecimal=True, dateFormat="yyyy-MM-dd",timestampFormat="yyyy-MM-dd HH:mm:ss",multiLine=False)
display(dfCog)

# COMMAND ----------

# DBTITLE 1,Flatten Json
dfCog = dfCog.select(col("id").alias("ID"), 
                     col("sentiment").alias("Sentiment"),
                     explode(array("confidenceScores.positive")).alias("Positive"),
                    col("confidenceScores.neutral").alias("Neutral"),
        col("confidenceScores.negative").alias("Negative")
                 )

dfCog = dfCog.select("ID", "Sentiment", "Positive", "Neutral", "Negative")
display(dfCog)

# COMMAND ----------

# DBTITLE 1,Join Original Dataset - Drop Mon Id Column
dfFinal = df.join(dfCog, df.ID == dfCog.ID, how="left").drop('ID')

# COMMAND ----------

# DBTITLE 1,Display Final Dataframe
display(dfFinal)
