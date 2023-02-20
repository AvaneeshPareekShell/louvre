# Databricks notebook source
# MAGIC %run /Louvre/Generic_Functions/connectionConfig/properties

# COMMAND ----------

#ADLS Connection - Configs
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": LouvreConfig["dataSets"]["Louvre_ADLS"]["Client_id"],
           "dfs.adls.oauth2.credential": dbutils.secrets.get(LouvreConfig["dataSets"]["Louvre_ADLS"]["Kv_scope"] , key = LouvreConfig["dataSets"]["Louvre_ADLS"]["kv_key"]),
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/"+LouvreConfig["dataSets"]["Louvre_ADLS"]["Tenant_Id"]+"/oauth2/token"}

# COMMAND ----------

# Function to mount ADLS
def mount_LV_ADLS():
  dbutils.fs.mount(
  source = LouvreConfig["dataSets"]["Louvre_ADLS"]["Storage_Account"],
  mount_point = LouvreConfig["dataSets"]["Louvre_ADLS"]["Mount_Path"],
  extra_configs = configs)

# COMMAND ----------

#mounting AMDP ADLS
try:
  mount_LV_ADLS()
  print("ADLS mounted")
except:
  print("ADLS already mounted")

# COMMAND ----------

def WriteFile_UnHarm(df,path):
    df.write\
      .format("delta")\
      .option("overwriteSchema", "true")\
      .mode("overwrite")\
      .save(path)

# COMMAND ----------

def ReadFile_UnHarm(spark,path):
    UnHarm_read_df = spark.read.format("delta") \
        .option("inferSchema", True) \
        .load(path)
    return UnHarm_read_df

# COMMAND ----------

def ReadFile_Land(spark,path):
    Land_read_df = spark.read.format("avro") \
        .option("inferSchema", True) \
        .load(path)
    return Land_read_df

# COMMAND ----------

def ReadFile_Raw(spark,path):
    Raw_read_df = spark.read.format("parquet") \
        .option("inferSchema","True") \
        .option("header","true")\
        .load(path)
    return Raw_read_df
