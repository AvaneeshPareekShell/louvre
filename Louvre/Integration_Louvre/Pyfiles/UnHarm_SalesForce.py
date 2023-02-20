# Databricks notebook source
# MAGIC %run /Louvre/Generic_Functions/connectionConfig/ADLSConnection

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/ADLSConnection

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/SQLConnector

# COMMAND ----------

dbutils.widgets.text("SF_Raw_Base_Folder","","")
SF_Raw_Base_Folder = dbutils.widgets.get("SF_Raw_Base_Folder")
dbutils.widgets.text("SF_UnHarm_Base_Folder","","")
SF_UnHarm_Base_Folder = dbutils.widgets.get("SF_UnHarm_Base_Folder")
dbutils.widgets.text("SF_Table_Name","","")
SF_Table_Name = dbutils.widgets.get("SF_Table_Name")

# COMMAND ----------

Mount_Path = LouvreConfig["dataSets"]["Louvre_ADLS"]["Mount_Path"]
Read_SF_Raw_Path = Mount_Path+SF_Raw_Base_Folder+SF_Table_Name
print(Read_SF_Raw_Path)
SF_Raw = ReadFile_Raw(spark,Read_SF_Raw_Path)

# COMMAND ----------

Mount_Path = LouvreConfig["dataSets"]["Louvre_ADLS"]["Mount_Path"]
Write_SF_UnHarm_Path = Mount_Path+SF_UnHarm_Base_Folder+SF_Table_Name+'/'
print(Write_SF_UnHarm_Path)
WriteFile_UnHarm(SF_Raw,Write_SF_UnHarm_Path)
