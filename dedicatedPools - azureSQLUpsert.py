def upsertAzureSQL(df, azureSqlStagingTable, azureSqlTargetTable, lookupColumns, deltaName):
    # ##########################################################################################################################  
    # Function: upsertAzureSQL
    # Performs a Merge/Upsert action on a Azure SQL table
    # 
    # Parameters:
    # df = Input dataframe
    # azureSqlStagingTable = Azure SQL Table used to stage the input dataframe for merge/upsert operation
    # azureSqlTargetTable = Azure SQL Target table where the dataframe is merged/upserted
    # lookupColumns = pipe separated columns that uniquely defines a record in input dataframe
    # deltaName = Name of watermark column in input dataframe
    # Author: https://www.linkedin.com/in/Data-Mastery/
    #
    # Returns:
    # None
    # ##########################################################################################################################
   #Source and Target Alias
  targetTableAlias = "Target"
  stagingTableAlias = "Source"

  #Azure SQL Connection Variables
  serverName = mssparkutils.credentials.getSecret('datamasterykeyvaultdev01','AzureSQL-ServerName')
  databaseName = mssparkutils.credentials.getSecret('datamasterykeyvaultdev01','AzureSQL-DatabaseName')
  sqlJDBC = "jdbc:sqlserver://" + serverName + ".database.windows.net:1433;databaseName=" + databaseName +";"
  userName = mssparkutils.credentials.getSecret('datamasterykeyvaultdev01','AzureSQL-Admin-Username')
  password = mssparkutils.credentials.getSecret('datamasterykeyvaultdev01','AzureSQL-Admin-Password')

  #Read Columns Names from Dataframe
  dfColumns = str(df.columns)
  dfColumns = (((dfColumns.replace("'", "")).replace("[","")).replace("]","")).replace(" ","")

  #Create a MERGE SQL for SCD UPSERT
  mergeStatement = "MERGE " + azureSqlTargetTable + " as " + targetTableAlias +  " USING " + azureSqlStagingTable \
    + " as " + stagingTableAlias + " ON ("

  #Generic Lookup Statement
  #When there are Lookup Columns
  if (lookupColumns is not None or lookupColumns is len(lookupColumns) > 0):
    uniqueCols = lookupColumns.split("|")
    lookupStatement = ""
    for lookupCol in uniqueCols:
      lookupStatement = lookupStatement + targetTableAlias + "." + lookupCol  + " = " + stagingTableAlias + "." + lookupCol + " and " 
       #print("insertSQL={}".format(insertSQL))

  #Update Merge Statement
  #When there is a delta column
  if deltaName is not None and  len(deltaName) >0:
    #Check if the last updated is greater than existing record
    updateStatement= lookupStatement + stagingTableAlias  +"."+ deltaName  + " >= "+ targetTableAlias + "." + deltaName

  else:
    #remove last "and"
    remove="and"
    reverse_remove=remove[::-1]
    updateStatement = lookupStatement[::-1].replace(reverse_remove,"",1)[::-1]



  if deltaName is not None and  len(deltaName) >0:
    #Check if the last updated is lesser than existing record
    updateStatement = updateStatement + " and " + targetTableAlias  +"."+ deltaName  + " < "+ stagingTableAlias + "." + deltaName

    #Add When Matched
  updateStatement = updateStatement + ") WHEN MATCHED THEN UPDATE SET "

  updateColumns = dfColumns.split(",")
  for lookupCol in updateColumns:
    updateStatement = updateStatement + targetTableAlias + "." + lookupCol  + " = " + stagingTableAlias + "." + lookupCol + ", " 
     #print("insertSQL={}".format(insertSQL))

  remove=","
  reverse_remove=remove[::-1]
  updateStatement = updateStatement[::-1].replace(reverse_remove,"",1)[::-1]+";"

  updateStatement = mergeStatement + updateStatement

  #Insert Statement
  remove="and"
  reverse_remove=remove[::-1]
  insertLookupStatement = lookupStatement[::-1].replace(reverse_remove,"",1)[::-1] +")"

  insertStatement = insertLookupStatement + " WHEN NOT MATCHED BY TARGET THEN INSERT (" + dfColumns.replace(",", ", ") + ") VALUES ("

  for lookupCol in updateColumns:
    insertStatement = insertStatement + stagingTableAlias + "." + lookupCol + ", "
     #print("insertSQL={}".format(insertSQL))

  remove=","
  reverse_remove=remove[::-1]
  insertStatement = insertStatement[::-1].replace(reverse_remove,"",1)[::-1] +");"

  #Form Insert Statement
  insertStatement = mergeStatement + insertStatement

  #Create Final Statement with Update Merge Statement and Insert Statement
  finalStatement = updateStatement + insertStatement

  # Due to a limiation with the Azure SQL Spark connector not accepting pre or post
  # SQL statements, we must use the PyODBC connector to execute the SQL Statements.
  import pyodbc
  conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                       'SERVER=' + serverName + '.database.windows.net;'
                       'DATABASE=' + databaseName + ';'
                       'UID=' + userName + ';'
                       'PWD='+ password + ';')
  cursor = conn.cursor()
  conn.autocommit = True
  
  #Truncate Staging Table before Inserting
  ##To overwrite the staging table each time remove or comment out the following line##
  cursor.execute("Truncate Table " + azureSqlStagingTable)
  
  #Write to Staging Table
  #Please change the mode to overwrite if you want the staging table to be created/overwritten each time
  df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("append") \
    .option("url", sqlJDBC) \
    .option("dbtable", azureSqlStagingTable) \
    .option("user", userName) \
    .option("password", password) \
    .option("schemaCheckEnabled", "false") \
    .save()


  #Execute Upsert Statement
  cursor.execute(finalStatement)

  conn.close()


  return finalStatement