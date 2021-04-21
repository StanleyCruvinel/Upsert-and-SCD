def type2SCD(df, outputDWStagingTable,outputDWTable, dateParameters):
# ##########################################################################################################################  
# Function: Type 2 SCD
# 
# Mandatory Parameters:
# df = input dataframe
# outputDWStagingTable = Azure Synapse/Datawarehouse Table used to stage the input dataframe for merge/upsert operation
# outputDWTable = Azure Synapse/Datawarehouse Target table where the dataframe is merged/upserted
# dateParameters = json string used to seperate columns and values
# Key Value Pair Options for dateParameters
# lookupColumns = pipe (|) separated columns that uniquely identifes a record in input dataframe.
# scd_effective_date_column = Input Date Column used to Data mine the SCD Start/End Dates
# scd_start_timestamp_column = Name of the Output Column
# scd_end_timestamp_column = Name of the Output Column
# scd_TimeStampFormat = Date Format of Input Date Column

# Author: https://www.linkedin.com/in/rorymcmanus/
# Blog: https://www.linkedin.com/company/Data-Mastery

# Returns:
# A dataframe is returned

  #Parallelize List
  scd_columns = spark.read.json(sc.parallelize([dateParameters]))
  scd_columns = scd_columns.select('scd_effective_date_column', 'scd_end_timestamp_column','scd_start_timestamp_column', 'scd_TimeStampFormat', 'lookupColumns').collect()
  
  #SCD Column List
  scd_columns_list = scd_columns[0].scd_effective_date_column + ", " + scd_columns[0].scd_start_timestamp_column + "," + scd_columns[0].scd_end_timestamp_column
  
  #SCD Columns In Variables
  scd_columns_StartTimeStamp = scd_columns[0].scd_start_timestamp_column
  scd_columns_EndTimeStamp = scd_columns[0].scd_end_timestamp_column
  scd_TimeStampFormat = scd_columns[0].scd_TimeStampFormat
  scd_effective_date_column = scd_columns[0].scd_effective_date_column
  
  #Please Extend this for Custom Formats
  if scd_TimeStampFormat in ("d/MM/yyyy"):
    df = df.withColumn(scd_columns_StartTimeStamp,date_format(to_date(col(scd_effective_date_column), scd_TimeStampFormat), "yyyy-MM-dd 00.00.00.0000000"))\
    .withColumn(scd_columns_EndTimeStamp, lit("9999-12-31 00.00.00.0000000"))
  
  #Generic Update Statement
  if (scd_columns_StartTimeStamp is not None or len(scd_columns_StartTimeStamp) > 0):
    updateStatement = "update " +  outputDWTable + " Set " + outputDWTable + "." + scd_columns_EndTimeStamp + "= DATEADD(ss, -1, Stg." + scd_columns_StartTimeStamp + ") from " + outputDWStagingTable + " stg where " + outputDWTable + "." + scd_columns_StartTimeStamp + " < stg." + scd_columns_StartTimeStamp + " and " + outputDWTable + "." + scd_columns_EndTimeStamp  + " = '9999-12-31' and "
  
  #When there are Lookup Columns
  if (lookupColumns is not None or lookupColumns is len(lookupColumns) > 0) and (outputDWTable is not None or len(outputDWTable) > 0) and (outputDWStagingTable is not None or len(outputDWStagingTable) > 0):
    uniqueCols = lookupColumns.split("|")
    finalStatement = ""
    for lookupCol in uniqueCols:
      finalStatement = finalStatement + outputDWTable + "." + lookupCol  + " = stg." + lookupCol + " and "
    finalStatement = updateStatement + finalStatement
  
  #When there are no Lookup Columns
  if (lookupColumns is None or len(lookupColumns) == 0):
      finalStatement = updateStatement
  
  #Build Post SQL Statement
  if (outputDWTable is not None or len(outputDWTable) > 0) and (outputDWStagingTable is not None or len(outputDWStagingTable) > 0):
   #Insert SQL
    insertSQL ="Insert Into " + outputDWTable + " select * from " + outputDWStagingTable +";"
     #print("insertSQL={}".format(insertSQL))
    remove="and"
    reverse_remove=remove[::-1]
    finalStatement = finalStatement[::-1].replace(reverse_remove,"",1)[::-1]
  
  postActionsSQL = finalStatement + " ; " + insertSQL
  
  
  
   #upsert to Synapse Analytics 
  insertDWH(df,outputDWTable,"append",outputDWStagingTable,None,postActionsSQL)
  
  return postActionsSQL
