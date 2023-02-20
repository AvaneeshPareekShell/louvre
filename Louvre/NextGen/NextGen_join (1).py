# Databricks notebook source
# MAGIC %run /Louvre/Generic_Functions/connectionConfig/ADLSConnection

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/properties

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/SQLConnector

# COMMAND ----------

from pyspark.sql import SparkSession, SQLContext
from time import gmtime, strftime, time
from pyspark.sql.functions import col,abs, to_timestamp, date_format,coalesce,regexp_replace,substring,trim,countDistinct
from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date,expr
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import count, avg,sum, length
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_date
from pyspark.sql.functions import concat
from pyspark.sql.functions import when,first,last
from pyspark.sql.functions import UserDefinedFunction
import datetime
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

df_mseg = ReadFile_UnHarm(spark,"/mnt/ADLS/PROJECT/P00137-GSAP_MATERIAL_MOVEMENT_PRE-PROD_UNHARM/PRE-PROD/GSAP_HANA.MSEG/")

# COMMAND ----------

df_mseg.display()

# COMMAND ----------

df_material_mapping = spark.read.format("csv").option("header","true").option("inferschema","true").load("/mnt/ADLS/RAW/W00746-LOUVRE_DEV_LAND/Material_Mapping/MaterialMapping.csv")

# COMMAND ----------

writeLouvreDbTable(df_material_mapping,"louvre_raw.material_mapping")

# COMMAND ----------

df_mseg.display()

# COMMAND ----------

df_mseg_Filtered = ReadFile(spark,"/mnt/ADLS/PROJECT/P00137-GSAP_MATERIAL_MOVEMENT_PRE-PROD_UNHARM/PRE-PROD/GSAP_HANA.MSEG/").select('OIHANTYP','kdpos','mblnr','kdauf','ebeln','matnr','bukrs','WERKS','zeile','ZXBLDAT','ZXBUDAT','BWART','OID_EXTBOL').dropDuplicates()
MSEGO2 = ReadFile(spark,"/mnt/ADLS/PROJECT/P00137-GSAP_MATERIAL_MOVEMENT_PRE-PROD_UNHARM/PRE-PROD/GSAP_HANA.MSEGO2/").select('MSEHI','ADQNT','mblnr','ZEILE').dropDuplicates()
EKKO = ReadFile(spark,"/mnt/ADLS/PROJECT/P00141-GSAP_PURCHASE_AGREEMENT_PRE-PROD_UNHARM/PRE-PROD/GSAP_HANA.EKKO/").select('aedat','ebeln','INCO1','INCO2').dropDuplicates()

# COMMAND ----------

#MSEG = ReadFile(spark,"/mnt/ADLS/PROJECT/P00115-GSAP_MATERIAL_MOVEMENT_DEV_UNHARM/CDD/GSAP_HANA.MSEG/").select('OIHANTYP','kdpos','mblnr','kdauf','ebeln','matnr','bukrs','WERKS','zeile','ZXBLDAT','ZXBUDAT','BWART','OID_EXTBOL').dropDuplicates()

#MSEGO2 = ReadFile(spark,"/mnt/ADLS/PROJECT/P00115-GSAP_MATERIAL_MOVEMENT_DEV_UNHARM/CDD/GSAP_HANA.MSEGO2/").select('MSEHI','ADQNT','mblnr','ZEILE').dropDuplicates()

#EKKO = ReadFile(spark,"/mnt/ADLS/PROJECT/P00118-GSAP_PURCHASE_AGREEMENT_DEV_UNHARM/CDD/GSAP_HANA.EKKO/").select('aedat','ebeln','INCO1','INCO2').dropDuplicates()

# COMMAND ----------

#taking only L15 and KG from msego2
li_msehi = ["L15","KG"]
df_msego2_filtered = MSEGO2.filter(MSEGO2.MSEHI.isin(li_msehi)).orderBy("MSEHI")

# COMMAND ----------

#grouping columns and getting KG and L15 values in separate columns
df_msego2_agg = df_msego2_filtered.groupBy("MBLNR","ZEILE") \
  .agg(first("ADQNT").alias("lifting_in_KG")  , last("ADQNT").alias("lifting_in_L15"))

# COMMAND ----------

df_msego2_quantity = df_msego2_agg.withColumn('lifting_in_KG',df_msego2_agg.lifting_in_KG*lit(-1)) \
                                  .withColumn('lifting_in_L15',df_msego2_agg.lifting_in_L15*lit(-1))

# COMMAND ----------

#Filtering MSEG on CompanyCode(bukrs), Movement Type Id(bwart)
#li_werks = ["A069"] plant code filter no more applicable
#li_bwart = ["101","102","601","602","301","302"]
#li_bukrs = ["DE01","AT01"]
#df_mseg_Filtered = MSEG.filter(MSEG.bukrs.isin(li_bukrs) & MSEG.BWART.isin(li_bwart))

# COMMAND ----------

df_msego2_quantity.display()

# COMMAND ----------

df_mergerd_MSEGO2 = df_mseg_Filtered.join(df_msego2_quantity,(df_mseg_Filtered.mblnr == df_msego2_quantity.MBLNR) & (df_mseg_Filtered.zeile == df_msego2_quantity.ZEILE),'left').drop(df_msego2_quantity.MBLNR).drop(df_msego2_quantity.ZEILE).drop_duplicates()

# COMMAND ----------

df_mergerd_EKKO = df_mergerd_MSEGO2.join(EKKO,df_mergerd_MSEGO2.ebeln == EKKO.ebeln,'left').drop(EKKO.ebeln).drop_duplicates()

# COMMAND ----------

df_mergerd_EKKO.display()

# COMMAND ----------

WriteFile_UnHarm(df_mergerd_EKKO,"/mnt/ADLS/PROJECT/P01361-LOUVRE_DEV_UNHARM/CDD/NextGen_join")

# COMMAND ----------

writeLouvreDbTable(df_mergerd_EKKO,"louvre_raw.l_NextGen_Join")

# COMMAND ----------


