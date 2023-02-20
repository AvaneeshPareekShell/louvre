# Databricks notebook source
# MAGIC %run /Louvre/Generic_Functions/connectionConfig/properties

# COMMAND ----------

def louvre_sql_server_jdbcConnection():
   #NextGen Curated PreProd SQLDB Connection
    
  kv_scope = LouvreConfig['dataSets']['louvre_db']['kv_scope']
  kv_key = LouvreConfig['dataSets']['louvre_db']['kv_key']  
  dbDatabase = LouvreConfig['dataSets']['louvre_db']['dbDatabase']
  dbServer = LouvreConfig['dataSets']['louvre_db']['dbServer']
  dbUser = LouvreConfig['dataSets']['louvre_db']['dbUser']
  dbPass = dbutils.secrets.get(scope = kv_scope, key = kv_key)
  dbJdbcPort = LouvreConfig['dataSets']['louvre_db']['dbJdbcPort']
  dbJdbcExtraOptions = LouvreConfig['dataSets']['louvre_db']['dbJdbcExtraOptions']
  torUrl = "jdbc:sqlserver://" + dbServer + ":" + dbJdbcPort + ";database=" + dbDatabase + ";user=" + dbUser+";password={"+dbPass+"}"
    
  return torUrl

# COMMAND ----------

def tc_sql_server_jdbcConnection():
   #NextGen Curated PreProd SQLDB Connection
    
  kv_scope = LouvreConfig['dataSets']['TC_db']['kv_scope']
  kv_key = LouvreConfig['dataSets']['TC_db']['kv_key']  
  dbDatabase = LouvreConfig['dataSets']['TC_db']['dbDatabase']
  dbServer = LouvreConfig['dataSets']['TC_db']['dbServer']
  dbUser = LouvreConfig['dataSets']['TC_db']['dbUser']
  dbPass = dbutils.secrets.get(scope = kv_scope, key = kv_key)
  dbJdbcPort = LouvreConfig['dataSets']['TC_db']['dbJdbcPort']
  dbJdbcExtraOptions = LouvreConfig['dataSets']['TC_db']['dbJdbcExtraOptions']
  torUrl = "jdbc:sqlserver://" + dbServer + ":" + dbJdbcPort + ";database=" + dbDatabase + ";user=" + dbUser+";password={"+dbPass+"}"
    
  return torUrl

# COMMAND ----------

def readLouvreDbTable(spark,Table):
    torUrlParam=louvre_sql_server_jdbcConnection()
    tor_read_df = spark.read.format("jdbc") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("url", torUrlParam) \
    .option("dbtable",Table) \
    .option("inferSchema", True) \
    .load()
    return tor_read_df

# COMMAND ----------

def readTcDbTable(spark,Table):
    torUrlParam=tc_sql_server_jdbcConnection()
    tor_read_df = spark.read.format("jdbc") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("url", torUrlParam) \
    .option("dbtable",Table) \
    .option("inferSchema", True) \
    .load()
    return tor_read_df

# COMMAND ----------

def writeLouvreDbTable(df,Table):
  torUrlParam=louvre_sql_server_jdbcConnection()
  df.write.mode("overwrite"). \
  format("jdbc"). \
  option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"). \
  option("url",torUrlParam). \
  option("dbtable",Table). \
  save()

# COMMAND ----------

def appendLouvreDbTable(df,Table):
  torUrlParam=louvre_sql_server_jdbcConnection()
  df.write.mode("overwrite"). \
  format("jdbc"). \
  option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"). \
  option("url",torUrlParam). \
  option("dbtable",Table). \
  save()
