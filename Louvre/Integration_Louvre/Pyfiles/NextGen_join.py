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
from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date,expr, ceil, current_timestamp
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

li_bukrs = ["DE01","AT01"]
Mount_Path = LouvreConfig["dataSets"]["Louvre_ADLS"]["Mount_Path"]
MSEG_PATH = Mount_Path+LouvreConfig["dataSets"]["l_ADLS_Loc"]["NextGen_Loc"]["MSEG"]+"GSAP_HANA.MSEG/"
MSEGO2_PATH = Mount_Path+LouvreConfig["dataSets"]["l_ADLS_Loc"]["NextGen_Loc"]["MSEGO2"]+"GSAP_HANA.MSEGO2/"
EKKO_PATH = Mount_Path+LouvreConfig["dataSets"]["l_ADLS_Loc"]["NextGen_Loc"]["EKKO"]+"GSAP_HANA.EKKO/"
MSEG = ReadFile_UnHarm(spark,MSEG_PATH).select('OIHANTYP','KDPOS','MBLNR','KDAUF','EBELN','MATNR','BUKRS','ZEILE','ZXBLDAT','ZXBUDAT','BWART','OID_EXTBOL','ZXOIB_BLTIME','CA_LAST_MDF','OIVBELN','OIC_PTRIP','OID_MISCDL','OIPOSNR','WERKS').dropDuplicates().withColumn("Nextgen_read_timestamp", current_timestamp()).withColumnRenamed("WERKS","MSEG_WERKS").withColumnRenamed("MATNR","MSEG_MATNR")
MSEGO2 = ReadFile_UnHarm(spark,MSEGO2_PATH).filter(col('BUKRS').isin(li_bukrs)).select('MSEHI','ADQNT','MBLNR','ZEILE').dropDuplicates()
EKKO = ReadFile_UnHarm(spark,EKKO_PATH).select('AEDAT','EBELN','INCO1','INCO2').dropDuplicates()

# COMMAND ----------

#taking only L15 and KG from msego2
li_msehi = ["L15","KG"]
df_msego2_filtered = MSEGO2.filter(MSEGO2.MSEHI.isin(li_msehi)).orderBy("MSEHI")

# COMMAND ----------

#grouping columns and getting KG and L15 values in separate columns
df_msego2_grouped = df_msego2_filtered.sort('MBLNR','ZEILE','MSEHI').groupBy("MBLNR","ZEILE").agg(sf.collect_list("MSEHI"),sf.collect_list("ADQNT")).withColumnRenamed("collect_list(MSEHI)", "UoM_List").withColumnRenamed("collect_list(ADQNT)", "Quantity")
df_msego2_agg = df_msego2_grouped.where((sf.col("UoM_List")[0]=='KG') & (sf.col("UoM_List")[1]=='L15')).withColumn("lifting_in_KG",df_msego2_grouped.Quantity[0]).withColumn("lifting_in_L15",df_msego2_grouped.Quantity[1]).drop('UoM_List').drop('Quantity')

# COMMAND ----------

df_msego2_quantity = df_msego2_agg.withColumn('lifting_in_KG',df_msego2_agg.lifting_in_KG*lit(-1)).withColumn('lifting_in_L15',df_msego2_agg.lifting_in_L15*lit(-1))

# COMMAND ----------

df_msego2_quantity_cbm_to = df_msego2_agg.withColumn('lifting_in_to',df_msego2_agg.lifting_in_KG/1000).withColumn('lifting_in_cbm',df_msego2_agg.lifting_in_L15/1000)

# COMMAND ----------

from datetime import datetime
current_timestamp = datetime.now()
year = str(current_timestamp.year)
start_month = str(current_timestamp.month-2)
end_month = str(current_timestamp.month)
start_date = "2022"+"09"+"01"
end_date = year+"0"+end_month+"31"
print(start_date)
print(end_date)
print(end_month)

# COMMAND ----------

#Filtering MSEG on CompanyCode(bukrs), Movement Type Id(bwart) and ZXBUDAT(loading date)
li_bwart = ["101","102","601","602","301","302"]
li_bukrs = ["DE01","AT01"]
df_mseg_Filtered = MSEG.filter((MSEG.BUKRS.isin(li_bukrs)) & (MSEG.BWART.isin(li_bwart)) & (MSEG.ZXBLDAT >= start_date) & (MSEG.ZXBLDAT <= end_date))

# COMMAND ----------

df_mseg_Filtered_select = df_mseg_Filtered.select('KDPOS','MBLNR','KDAUF')

# COMMAND ----------

df_NextGen_EKKO = df_mseg_Filtered.join(EKKO,df_mseg_Filtered.EBELN == EKKO.EBELN,'left').drop(EKKO.EBELN)

# COMMAND ----------

df_NextGen_mergerd = df_NextGen_EKKO.join(df_msego2_quantity_cbm_to,(df_NextGen_EKKO.MBLNR == df_msego2_quantity_cbm_to.MBLNR) & (trim(df_NextGen_EKKO.ZEILE) == trim(df_msego2_quantity_cbm_to.ZEILE)),'left').drop(df_msego2_quantity_cbm_to.MBLNR).drop(df_msego2_quantity_cbm_to.ZEILE)

# COMMAND ----------

# changing lifiting values for change destination after pickup based on bwart
li_bwart_CD = ["102","602","302"]
df_NextGen_mergerd_final = df_NextGen_mergerd.withColumn("lifting_in_KG", when(df_NextGen_mergerd.BWART.isin(li_bwart_CD),df_NextGen_mergerd.lifting_in_KG*lit(-1)).otherwise(df_NextGen_mergerd.lifting_in_KG))\
                  .withColumn("lifting_in_L15", when(df_NextGen_mergerd.BWART.isin(li_bwart_CD),df_NextGen_mergerd.lifting_in_L15*lit(-1)).otherwise(df_NextGen_mergerd.lifting_in_L15)) \
                  .withColumn("lifting_in_cbm", when(df_NextGen_mergerd.BWART.isin(li_bwart_CD),df_NextGen_mergerd.lifting_in_cbm*lit(-1)).otherwise(df_NextGen_mergerd.lifting_in_cbm)) \
                  .withColumn("lifting_in_to", when(df_NextGen_mergerd.BWART.isin(li_bwart_CD),df_NextGen_mergerd.lifting_in_to*lit(-1)).otherwise(df_NextGen_mergerd.lifting_in_to)) \
                  .withColumn("Lifting_Period", concat(substring("ZXBLDAT",0,4),lit('-'),substring("ZXBLDAT",5,2)))

# COMMAND ----------

#writeLouvreDbTable(df_NextGen_mergerd_final,"louvre_curated.NextGen_Join")
