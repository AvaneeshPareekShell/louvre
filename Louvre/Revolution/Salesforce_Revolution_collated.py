# Databricks notebook source
# MAGIC %run /Louvre/Generic_Functions/connectionConfig/properties

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/ADLSConnection

# COMMAND ----------

#fetching all tables
df_amv = ReadFile(spark,'/mnt/ADLS/PROJECT/P01359-REVOLUTION_SALES_ORDER_DEV_UNHARM/CDD/l_Price_Audit_Backbone_AMV__c')
df_bsp = ReadFile(spark,'/mnt/ADLS/PROJECT/P01359-REVOLUTION_SALES_ORDER_DEV_UNHARM/CDD/l_Price_Audit_BSP__c')
df_sht = ReadFile(spark,'/mnt/ADLS/PROJECT/P01359-REVOLUTION_SALES_ORDER_DEV_UNHARM/CDD/l_SHT__c')

# COMMAND ----------

df_amv.display()

# COMMAND ----------

df_bsp.display()

# COMMAND ----------

df_bsp.count()

# COMMAND ----------

df_sht.display()

# COMMAND ----------

df_sht.count()

# COMMAND ----------

#joining them together on MRC Number
df_bsp_amv = df_bsp.join(df_amv, df_bsp.Mrc_No__c == df_amv.Mrc_No__c).drop(df_bsp.Mrc_No__c)

# COMMAND ----------

df_bsp_amv.count()

# COMMAND ----------

df_sht_bsp.dropDuplicates()

# COMMAND ----------

df_sht_bsp.count()

# COMMAND ----------

df_sht_bsp.display()

# COMMAND ----------

df_sht_bsp_amv = df_sht_bsp.join(df_amv, df_sht_bsp.MRC_Number__c == df_amv.Mrc_No__c, "left").drop(df_amv.Mrc_No__c)

# COMMAND ----------

df = df_sht_bsp_amv.dropDuplicates()

# COMMAND ----------

df.cache()

# COMMAND ----------

df.dropDuplicates()

# COMMAND ----------

df_sht_bsp_amv.display()

# COMMAND ----------

df.count()

# COMMAND ----------


