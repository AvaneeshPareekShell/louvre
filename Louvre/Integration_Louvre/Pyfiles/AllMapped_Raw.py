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
from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date,expr, row_number, add_months, concat_ws, current_timestamp
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

#spark = SparkSession\
# .builder\
# .appName('Louvre')\
# .getOrCreate()

#spark.conf.set("spark.sql.shuffle.partitions",'10')

# COMMAND ----------

li_vkorg = ["DE01","AT01"]
li_vtweg = ["02"]
li_auart = ["ZCQ","ZCRM"]
Mount_Path = LouvreConfig["dataSets"]["Louvre_ADLS"]["Mount_Path"]
VBAK_PATH = Mount_Path+"/PROJECT/P00042-GSAP_SALES_AGREEMENT_PROD_UNHARM/PROD/GSAP_HANA.VBAK"
VBAP_PATH = Mount_Path+"/PROJECT/P00042-GSAP_SALES_AGREEMENT_PROD_UNHARM/PROD/GSAP_HANA.VBAP"
VBFA_PATH = Mount_Path+"/PROJECT/P00042-GSAP_SALES_AGREEMENT_PROD_UNHARM/PROD/GSAP_HANA.VBFA"
VBKD_PATH = Mount_Path+"/PROJECT/P00042-GSAP_SALES_AGREEMENT_PROD_UNHARM/PROD/GSAP_HANA.VBKD"
T001W_PATH = Mount_Path+"/PROJECT/P00043-GSAP_SITE_PROD_UNHARM/PROD/GSAP_HANA.T001W"
KNA1_PATH = Mount_Path+"/PROJECT/P00035-GSAP_LEGAL_ENTITY_PROD_UNHARM/PROD/GSAP_HANA.KNA1"
KONV_PATH = Mount_Path+"/PROJECT/P00039-GSAP_PRICE_CONDITIONS_PROD_UNHARM/PROD/GSAP_HANA.KONV"
VBAK = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBAK").select('vbeln','guebg','gueen','erdat','erzet','ctlpc','auart','vsbed','vtweg','kunnr','knumv','ktext','vkorg','batch_time').dropDuplicates().withColumn("tc_read_timestamp", current_timestamp())
VBAP = ReadFile_UnHarm(spark,VBAP_PATH).filter((col("BUKRS").isin(li_vkorg))).select('MATNR','VOLUM','OID_SHIP','posnr','vbeln','vbap_werks','UMZIN','abgru','voleh','ZMENG','ZIEME','bukrs').dropDuplicates().withColumnRenamed("bukrs","VBAP_bukrs")
KNA1 = ReadFile_UnHarm(spark,KNA1_PATH).select('NAME1','kunnr').dropDuplicates()
VBRP = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBRP").filter((col("vtweg").isin(li_vtweg)) & (col("vkorg").isin(li_vkorg))).select('VBELN','POSNR','AUBEL','oid_miscdl','vkorg').dropDuplicates().withColumnRenamed("POSNR","VBRP_Position_Number").withColumnRenamed("VBELN","Invoice_Number").withColumnRenamed("oid_miscdl","VBRP_Sales_Contract_Number")
VBKD = ReadFile_UnHarm(spark,VBKD_PATH).filter((col("BUKRS").isin(li_vkorg))).select('BSARK_E','vbeln','posnr').dropDuplicates().withColumnRenamed("posnr","VBRP_posnr")
VBFA = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBFA").filter((col("vtweg").isin(li_vtweg)) & (col("vkorg").isin(li_vkorg))).select('vbeln','vbelv','posnv').dropDuplicates().withColumnRenamed("vbeln","MSEG_SO").withColumnRenamed("posnv","VBFA_posnv")
VBRK = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBRK").filter((col("vtweg").isin(li_vtweg)) & (col("vkorg").isin(li_vkorg))).select('vbeln','knumv','FKART','ZTERM','ZLSCH','ERDAT').dropDuplicates().withColumnRenamed("knumv","vbrk_knumv").withColumnRenamed("ERDAT","Billing_Date")
KONV = ReadFile_UnHarm(spark,KONV_PATH).filter(col("BUKRS").isin(li_vkorg)).select('kschl','knumv','kposn').dropDuplicates()
KNVP = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.KNVP").filter((col("vtweg").isin(li_vtweg)) & (col("vkorg").isin(li_vkorg))).select('kunn2','kunnr').dropDuplicates()
KNVV = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.KNVV").dropDuplicates()
mrd_material = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.mrd_material").select('PROD_GROUP_DESC','matnr','PROD_SUBGROUP','GRADE').dropDuplicates()
t001w_cc = ReadFile_UnHarm(spark,T001W_PATH).select('WERKS','NAME1').withColumnRenamed("WERKS","Plant").withColumnRenamed("NAME1","Plant_Name").dropDuplicates()
oiklidr = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.oiklidr").select('werks','lidno','lid3cod1','docno','itemno').dropDuplicates()


# COMMAND ----------

VBFA_MBLNR = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBFA")
VBFA_MBLNR_filter = VBFA_MBLNR.select('vbeln','vbelv').dropDuplicates().withColumnRenamed("vbeln","Material_DN")

# COMMAND ----------

#filtering out data on Distribution Channel(VTWEG) = '02'(Commercial Fuels) & Sales Document Type(AUART)
li_vkorg = ["DE01","AT01"]
li_vtweg = ["02"]
li_auart = ["ZCQ","ZCRM"] #remvoed auart filter as per thorsten suggestion 
df_vbak_Filtered = VBAK.filter((VBAK.vtweg.isin(li_vtweg)) & (VBAK.auart.isin(li_auart)) & (VBAK.vkorg.isin(li_vkorg)))

# COMMAND ----------

from pyspark.sql.window import *
df_vbak_Filtered_agg = df_vbak_Filtered.select("vbeln","guebg","gueen","erdat","erzet","ctlpc","auart","vsbed","vtweg","kunnr","knumv","KTEXT","batch_time").withColumn("Row_number",row_number().over(Window.partitionBy("vbeln","erdat","erzet","ctlpc","auart","vsbed","vtweg","kunnr","knumv").orderBy(df_vbak_Filtered.batch_time.desc())))
df_vbak_Filtered_agg_MaxDate = df_vbak_Filtered_agg.filter(df_vbak_Filtered_agg.Row_number == 1)

# COMMAND ----------

from datetime import datetime
current_timestamp = datetime.now()
year = str(current_timestamp.year)
start_month = str(current_timestamp.month-2)
end_month = str(current_timestamp.month)
end_date = year+"12"+"31"
print(end_date)
print(end_month)

# COMMAND ----------

# filering gueen between -100 and +230 days
df_vbak_Filtered_gueen = df_vbak_Filtered_agg_MaxDate.filter((df_vbak_Filtered_agg_MaxDate.gueen >= date_format(sf.date_add(to_date(current_date(),'yyyyMMdd'),-100),'yyyyMMdd')) & (df_vbak_Filtered_agg_MaxDate.gueen <= end_date))

# COMMAND ----------

# filering gueen between -100 and +230 days
#df_vbak_Filtered_gueen = df_vbak_Filtered_agg_MaxDate.filter((df_vbak_Filtered_agg_MaxDate.gueen >= date_format(sf.date_add(to_date(current_date(),'yyyyMMdd'),-100),'yyyyMMdd')) & (df_vbak_Filtered_agg_MaxDate.gueen <= date_format(sf.date_add(to_date(current_date(),'yyyyMMdd'),+230),'yyyyMMdd')))

# COMMAND ----------

df_vbak_Filtered_ktext_agg = df_vbak_Filtered_gueen.groupBy("vbeln","guebg","gueen","erdat","erzet","ctlpc","auart","vsbed","vtweg","kunnr","knumv").agg(sf.collect_list("KTEXT")).withColumn("KTEXT", concat_ws(", ", "collect_list(KTEXT)")).drop("collect_list(KTEXT)")

# COMMAND ----------

# filtering the nulls from KONV and VBKD to avoid multiple records (few contracts are getting both nulls and values)
VBKD_Filtered = VBKD.filter(length(VBKD.BSARK_E) != 0)
KONV_Filtered = KONV.filter(length(KONV.kschl) != 0)
KNA1_Filtered = KNA1.filter(length(KNA1.NAME1) != 0)

# COMMAND ----------

Pricing_schemes = ['YP09','YP10','YP03','YP04','YP23','YP24']
df_konv = KONV_Filtered.filter(KONV_Filtered.kschl.isin(Pricing_schemes))

# COMMAND ----------

term = ['YP03','YP04']
termman = ['YP09','YP10']
spot = ['YP23','YP24']
df_konv_dealtype = df_konv.withColumn('pricing_scheme', when(df_konv.kschl.isin(term),'TERM').when(df_konv.kschl.isin(spot),'SPOT').when(df_konv.kschl.isin(termman),'TERMMAN').otherwise(''))

# COMMAND ----------

# Join VBAK and VBAP
df_vbak_vbap = df_vbak_Filtered_ktext_agg.join(VBAP,df_vbak_Filtered_ktext_agg["vbeln"] ==  VBAP["vbeln"],"left").drop(VBAP.vbeln)

# COMMAND ----------

VBFA_322900021 = spark.createDataFrame(
    [
        ('0276091138', "0322900021", "2022-12-13 14:02:53" )
    ],
    ["MSEG_SO", "vbelv", "VBFA_batch_time"]
)

# COMMAND ----------

VBFA = VBFA.unionAll(VBFA_322900021)

# COMMAND ----------

# Join VBAK,VBAP,VBFA
df_mergerd_vbfa = df_vbak_vbap.join(VBFA,df_vbak_vbap["vbeln"] ==  VBFA["vbelv"],"left").drop(VBFA.vbelv)

# COMMAND ----------

df_mergerd_mblnr = df_mergerd_vbfa.join(VBFA_MBLNR_filter,df_mergerd_vbfa["MSEG_SO"] ==  VBFA_MBLNR_filter["vbelv"],"left").drop(VBFA_MBLNR_filter.vbelv)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,vbkd
df_mergerd_vbkd = df_mergerd_mblnr.join(VBKD_Filtered,(df_mergerd_mblnr["vbeln"] ==  VBKD_Filtered["vbeln"]) & (df_mergerd_mblnr["posnr"] ==  VBKD_Filtered["VBRP_posnr"]),"left").drop(VBKD_Filtered.vbeln)

# COMMAND ----------

# MAGIC %run /Louvre/Integration_Louvre/Pyfiles/NextGen_join

# COMMAND ----------

df_SalesORder_MD = df_mergerd_vbkd.join(df_mseg_Filtered_select,(df_mergerd_vbkd["MSEG_SO"] == df_mseg_Filtered_select["KDAUF"]) & (df_mergerd_vbkd["posnr"] == df_mseg_Filtered_select["KDPOS"]),'left').withColumn("MBLNR", when(df_mseg_Filtered_select.MBLNR.isNull(),df_mergerd_vbkd.Material_DN).otherwise(df_mseg_Filtered_select.MBLNR)).drop(df_mseg_Filtered_select.KDAUF).drop(df_mseg_Filtered_select.MBLNR).drop(df_mseg_Filtered_select.KDPOS)

# COMMAND ----------

df_merged_TC_NextGen_Join = df_SalesORder_MD.join(df_NextGen_mergerd_final,(df_SalesORder_MD["MBLNR"] == df_NextGen_mergerd_final["MBLNR"]) & (df_SalesORder_MD["posnr"] == df_NextGen_mergerd_final["OIPOSNR"]),'left').drop(df_NextGen_mergerd_final.KDAUF).drop(df_NextGen_mergerd_final.KDPOS).drop(df_NextGen_mergerd_final.MBLNR).filter(df_NextGen_mergerd_final.ZXBLDAT.isNotNull() & df_mergerd_vbkd.MATNR.isNotNull())
#df_merged_TC_NextGen_Join = df_mergerd_vbkd.join(df_NextGen_mergerd_final,(df_mergerd_vbkd["Material_DN"] == df_NextGen_mergerd_final["MBLNR"]) & (df_mergerd_vbkd["posnr"] == df_NextGen_mergerd_final["OIPOSNR"]),'left').filter(df_NextGen_mergerd_final.ZXBLDAT.isNotNull() & df_mergerd_vbkd.MATNR.isNotNull()).drop(df_NextGen_mergerd_final.KDAUF).drop(df_NextGen_mergerd_final.KDPOS)

# COMMAND ----------

li_bwart_CD = ["102","602","302"]
df_merged_TC_NextGen = df_merged_TC_NextGen_Join.withColumn("posnr", when(df_merged_TC_NextGen_Join.BWART.isin(li_bwart_CD),df_merged_TC_NextGen_Join.VBFA_posnv).otherwise(df_merged_TC_NextGen_Join.posnr)).withColumn("MATNR", when(df_merged_TC_NextGen_Join.BWART.isin(li_bwart_CD),df_merged_TC_NextGen_Join.MSEG_MATNR).otherwise(df_merged_TC_NextGen_Join.MATNR)).withColumn("vbap_werks", when(df_merged_TC_NextGen_Join.BWART.isin(li_bwart_CD),df_merged_TC_NextGen_Join.MSEG_WERKS).otherwise(df_merged_TC_NextGen_Join.vbap_werks))

# COMMAND ----------

# adding Contract_Start_Period and Contract_End_Period
df_merged_TC_NextGen_add_Fields = df_merged_TC_NextGen.withColumn('Contract_Start_Period',concat(substring("GUEBG",0,4),lit('-'),substring("GUEBG",5,2))).\
                          withColumn('Contract_End_Period',concat(substring("GUEEN",0,4),lit('-'),substring("GUEEN",5,2)))

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1(sold to name)
df_mergerd_kna1_soldname = df_merged_TC_NextGen_add_Fields.join(KNA1_Filtered,df_merged_TC_NextGen_add_Fields["kunnr"] ==  KNA1_Filtered["kunnr"],"left").drop(KNA1_Filtered.kunnr).withColumnRenamed("name1","Sold_To_Name")

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1(ship to name)
df_mergerd_kna1_shipname = df_mergerd_kna1_soldname.join(KNA1,df_mergerd_kna1_soldname["OID_SHIP"] ==  KNA1["kunnr"],"left").drop(KNA1.kunnr).withColumnRenamed("name1","Ship_To_Name")

# COMMAND ----------

df_mergerd_VBRP = df_mergerd_kna1_shipname.join(VBRP,df_mergerd_kna1_shipname["MSEG_SO"] ==  VBRP["AUBEL"],"left").drop(VBRP.AUBEL)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,VBKD,KONV,MED_MATERIAL,T001W_CC,VBRP,VBRK
df_mergerd_VBRK = df_mergerd_VBRP.join(VBRK,df_mergerd_VBRP["Invoice_Number"] ==  VBRK["vbeln"],"left").drop(VBRK.vbeln)

# COMMAND ----------

#join KONV table to get pricing scheme
df_mergerd_konv = df_mergerd_VBRK.join(df_konv_dealtype,(df_mergerd_VBRK["knumv"] ==  df_konv_dealtype["knumv"]) & (df_mergerd_VBRK["posnr"] == df_konv_dealtype["kposn"]),"left").drop(df_konv_dealtype.knumv)

# COMMAND ----------

df_mergerd_BSARK_Update = df_mergerd_konv.withColumn("BSARK_E", when(((df_mergerd_konv.BSARK_E.isNull()) | (length(df_mergerd_konv.BSARK_E) == 0)) & (df_mergerd_konv.auart == 'ZCRM') ,'TRRM').when(((df_mergerd_konv.BSARK_E.isNull()) | (length(df_mergerd_konv.BSARK_E) == 0)) & (df_mergerd_konv.auart != 'ZCRM') & (df_mergerd_konv.pricing_scheme == 'TERM'),'TTFD').when(((df_mergerd_konv.BSARK_E.isNull()) | (length(df_mergerd_konv.BSARK_E) == 0)) & (df_mergerd_konv.auart != 'ZCRM') & (df_mergerd_konv.pricing_scheme == 'SPOT'),'TSFP').when(((df_mergerd_konv.BSARK_E.isNull()) | (length(df_mergerd_konv.BSARK_E) == 0)) & (df_mergerd_konv.auart != 'ZCRM') & (df_mergerd_konv.pricing_scheme == 'TERMMAN'),'TSFP').when(((df_mergerd_konv.BSARK_E.isNull()) | (length(df_mergerd_konv.BSARK_E) == 0)) & (df_mergerd_konv.auart != 'ZCRM'),'TTFD').otherwise(df_mergerd_konv.BSARK_E))

# COMMAND ----------

df_merged_TC_NextGen_Austria_Contracts = df_mergerd_BSARK_Update.withColumn("Contract_Start_Period", when(df_merged_TC_NextGen_add_Fields.vbeln.isin('0322446905','0322581121','0322581137','0322581138','0322581181','0322581184','0322581186','0322581205','0322581206','0322581207','0322581210','0322581211','0322581213','0322581214','0322581338','0322581457','0322583862') & df_mergerd_BSARK_Update.BSARK_E.isin('TTFD','TTTT','TTTS','TTTM','TTTI'),df_mergerd_BSARK_Update.Lifting_Period).otherwise(df_mergerd_BSARK_Update.Contract_Start_Period))

# COMMAND ----------

# trim the leading o's for material number to match with mrd_material tables
df_merged_TC_NextGen_select_rn = df_merged_TC_NextGen_Austria_Contracts.withColumn("tr_matnr",sf.regexp_replace('MATNR', r'^[0]*', ''))

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,VBKD,KONV,MED_MATERIAL
df_mergerd_mrd_material = df_merged_TC_NextGen_select_rn.join(mrd_material,df_merged_TC_NextGen_select_rn["tr_matnr"] ==  mrd_material["matnr"],"left").drop(mrd_material.matnr).drop(df_merged_TC_NextGen_select_rn.tr_matnr)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,VBKD,KONV,MED_MATERIAL,T001W_CC
TC_NextGen_Merged = df_mergerd_mrd_material.join(t001w_cc,df_mergerd_mrd_material["vbap_werks"] ==  t001w_cc["Plant"],"left").drop(t001w_cc.Plant)

# COMMAND ----------

TC_NextGen_Merged_RejDeals = TC_NextGen_Merged.filter(length(trim(TC_NextGen_Merged.abgru)) != 0)

# COMMAND ----------

TC_NextGen_Merged_Final = TC_NextGen_Merged.exceptAll(TC_NextGen_Merged_RejDeals)

# COMMAND ----------

AllMapped_Raw = TC_NextGen_Merged_Final.drop('UMZIN').drop('voleh').drop('VOLUM').drop('zeile').drop('zxbudat').drop('kposn').drop('vkgrp').dropDuplicates()

# COMMAND ----------

#writeLouvreDbTable(AllMapped_Raw,"louvre_curated.AllMapped_Raw")
