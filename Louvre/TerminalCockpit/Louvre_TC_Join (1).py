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

df_vbak_Filtered = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBAK").select('vbeln','guebg','gueen','vkgrp','erdat','erzet','ctlpc','auart','vsbed','vtweg','oid_miscdl','kunnr','knumv','ktext').dropDuplicates() #'ktext' to be added when available
#VBAK = ReadFile(spark,"/mnt/ADLS/PROJECT/P00124-GSAP_SALES_AGREEMENT_DEV_UNHARM/CDD/GSAP_HANA.VBAK").select('vbeln','guebg','gueen','erdat','vsbed','vtweg','kunnr','knumv').dropDuplicates()
VBAP = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBAP").select('MATNR','VOLUM','OID_SHIP','posnr','vbeln','werks_1','UMZIN').dropDuplicates()
KNA1 = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.KNA1").select('NAME1','kunnr').dropDuplicates()
VBRP = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBRP").select('VBELN','POSNR','AUBEL','OIC_PTRIP').dropDuplicates().withColumnRenamed("POSNR","Position_Number")
VBKD = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBKD").select('BSARK_E','vbeln').dropDuplicates()
VBFA = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBFA").select('vbeln','vbelv').dropDuplicates().withColumnRenamed("vbeln","Delivery_Number")
VBRK = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.VBRK").select('vbeln','knumv','FKART','ZTERM','ZLSCH').dropDuplicates()
KONV = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.KONV").select('kschl','knumv','kposn').dropDuplicates()
KNVP = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.KNVP").select('kunn2','kunnr').dropDuplicates()
mrd_material = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.mrd_material").select('PROD_GROUP_DESC','matnr','PROD_SUBGROUP','GRADE').dropDuplicates()
t001w_cc = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.t001w_cc").select('plant','Plant_Name').dropDuplicates()

# COMMAND ----------

#filtering out data on Distribution Channel(VTWEG) = '02'(Commercial Fuels) & Sales Document Type(AUART)
#li_vtweg = ["02"]
#li_auart = ["ZCQ","ZCRM"]
#df_vbak_Filtered = VBAK.filter(VBAK.vtweg.isin(li_vtweg) & VBAK.auart.isin(li_auart))
#df_vbak_Filtered = VBAK.filter(VBAK.vtweg.isin(li_vtweg))

# COMMAND ----------

Pricing_schemes = ['YP09','YP10','YP03','YP04','YP23','YP24']
df_konv = KONV.filter(KONV.kschl.isin(Pricing_schemes))

# COMMAND ----------

term = ['YP03','YP04']
termman = ['YP09','YP10']
spot = ['YP23','YP24']
df_konv_dealtype = df_konv.withColumn('pricing_scheme', when(df_konv.kschl.isin(term),'TERM').when(df_konv.kschl.isin(spot),'SPOT').when(df_konv.kschl.isin(termman),'TERMMAN').otherwise(''))

# COMMAND ----------

# Join VBAK and VBAP
df_vbak_vbap = df_vbak_Filtered.join(VBAP,df_vbak_Filtered["vbeln"] ==  VBAP["vbeln"],"left").drop(VBAP.vbeln)

# COMMAND ----------

# Join VBAK,VBAP,VBFA
df_mergerd_vbfa = df_vbak_vbap.join(VBFA,df_vbak_vbap["vbeln"] ==  VBFA["vbelv"],"left").drop(VBFA.vbelv)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1(sold to name)
df_mergerd_kna1_soldname = df_mergerd_vbfa.join(KNA1,df_mergerd_vbfa["kunnr"] ==  KNA1["kunnr"],"left").drop(KNA1.kunnr).withColumnRenamed("name1","Sold_To_Name")

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1(ship to name)
df_mergerd_kna1_shipname = df_mergerd_kna1_soldname.join(KNA1,df_mergerd_kna1_soldname["OID_SHIP"] ==  KNA1["kunnr"],"left").drop(KNA1.kunnr).withColumnRenamed("name1","Ship_To_Name")

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1 with KNVP to get kunnr2 for national account name
df_mergerd_KNVP = df_mergerd_kna1_shipname.join(KNVP,df_mergerd_kna1_shipname["KUNNR"] ==  KNVP["KUNNR"],"left").drop(KNVP.kunnr)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNVP,KNA1(national account name)
df_mergerd_kna1_NAN = df_mergerd_KNVP.join(KNA1,df_mergerd_KNVP["kunn2"] ==  KNA1["kunnr"],"left").drop(KNA1.kunnr).withColumnRenamed("name1","National_Account_Name")

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,vbkd
df_mergerd_vbkd = df_mergerd_kna1_NAN.join(VBKD,df_mergerd_kna1_NAN["vbeln"] ==  VBKD["vbeln"],"left").drop(VBKD.vbeln)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,VBKD,KONV
df_mergerd_konv = df_mergerd_vbkd.join(df_konv_dealtype,df_mergerd_vbkd["knumv"] ==  df_konv_dealtype["knumv"],"left").drop(df_konv_dealtype.knumv).drop(df_konv_dealtype.knumv)

# COMMAND ----------

# trim the leading o's for material number to match with mrd_material tables
df_mergerd_konv = df_mergerd_konv.withColumn("tr_matnr",sf.regexp_replace('MATNR', r'^[0]*', ''))

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,VBKD,KONV,MED_MATERIAL
df_mergerd_mrd_material = df_mergerd_konv.join(mrd_material,df_mergerd_konv["tr_matnr"] ==  mrd_material["matnr"],"left").drop(mrd_material.matnr).drop(df_mergerd_konv.kschl)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,VBKD,KONV,MED_MATERIAL,T001W_CC
df_mergerd_t001w_cc = df_mergerd_mrd_material.join(t001w_cc,df_mergerd_mrd_material["werks_1"] ==  t001w_cc["plant"],"left").drop(t001w_cc.plant)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,VBKD,KONV,MED_MATERIAL,T001W_CC,VBRP
df_mergerd_VBRP = df_mergerd_t001w_cc.join(VBRP,df_mergerd_t001w_cc["VBELN"] ==  VBRP["AUBEL"],"left").drop(VBRP.AUBEL).drop(VBRP.VBELN)

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1,VBKD,KONV,MED_MATERIAL,T001W_CC,VBRP,VBRK
df_mergerd_VBRK = df_mergerd_VBRP.join(VBRK,df_mergerd_VBRP["vbeln"] ==  VBRK["vbeln"],"left").drop(VBRK.vbeln).drop(VBRK.knumv)

# COMMAND ----------

df_mergerd_VBRK = df_mergerd_VBRK.dropDuplicates()

# COMMAND ----------

df_mseg_Filtered = ReadFile(spark,"/mnt/ADLS/PROJECT/P00137-GSAP_MATERIAL_MOVEMENT_PRE-PROD_UNHARM/PRE-PROD/GSAP_HANA.MSEG/").select('OIHANTYP','kdpos','mblnr','kdauf','ebeln','matnr','bukrs','WERKS','zeile','ZXBLDAT','ZXBUDAT','BWART','OID_EXTBOL').dropDuplicates()
MSEGO2 = ReadFile(spark,"/mnt/ADLS/PROJECT/P00137-GSAP_MATERIAL_MOVEMENT_PRE-PROD_UNHARM/PRE-PROD/GSAP_HANA.MSEGO2/").select('MSEHI','ADQNT','mblnr','ZEILE').dropDuplicates()
EKKO = ReadFile(spark,"/mnt/ADLS/PROJECT/P00141-GSAP_PURCHASE_AGREEMENT_PRE-PROD_UNHARM/PRE-PROD/GSAP_HANA.EKKO/").select('aedat','ebeln','INCO1','INCO2').dropDuplicates()

# COMMAND ----------

#taking only L15 and KG from msego2
li_msehi = ["L15","KG"]
df_msego2_filtered = MSEGO2.filter(MSEGO2.MSEHI.isin(li_msehi)).orderBy("MSEHI")

# COMMAND ----------

#grouping columns and getting KG and L15 values in separate columns
df_msego2_agg = df_msego2_filtered.groupBy("MBLNR","ZEILE") \
  .agg(first("ADQNT").alias("lifting_in_KG")  , last("ADQNT").alias("lifting_in_L15"))

# COMMAND ----------

df_msego2_quantity = df_msego2_agg.withColumn('lifting_in_KG',df_msego2_agg.lifting_in_KG*(-1)) \
                                  .withColumn('lifting_in_L15',df_msego2_agg.lifting_in_L15*(-1))

# COMMAND ----------

df_mergerd_MSEGO2 = df_mseg_Filtered.join(df_msego2_quantity,(df_mseg_Filtered.mblnr == df_msego2_quantity.MBLNR) & (df_mseg_Filtered.zeile == df_msego2_quantity.ZEILE),'left').drop(df_msego2_quantity.MBLNR).drop(df_msego2_quantity.ZEILE).drop_duplicates()

# COMMAND ----------

df_merged_TC_NextGen = df_mergerd_VBRK.join(df_mergerd_MSEGO2,(df_mergerd_VBRK["vbeln"] == df_mergerd_MSEGO2["kdauf"]) & (df_mergerd_VBRK["posnr"] == df_mergerd_MSEGO2["kdpos"])).drop(df_mergerd_MSEGO2.kdauf).drop(df_mergerd_MSEGO2.kdpos).drop(df_mergerd_VBRK.MATNR)

# COMMAND ----------

df_merged_TC_NextGen.coalesce(3).display()

# COMMAND ----------

df_merged_TC_NextGen.display()

# COMMAND ----------

df_merged_TC_NextGen.coalesce(3).write.format("delta").mode("overwrite").save("/mnt/ADLS/PROJECT/P01361-LOUVRE_DEV_UNHARM/CDD/TC_NextGen_Join/")

# COMMAND ----------

df_merged_TC_NextGen.rdd.getNumPartitions()

# COMMAND ----------

df_merged_TC_NextGen.display()

# COMMAND ----------

#writeLouvreDbTable(df_mergerd_VBRK,"louvre_raw.l_TC_Join")

# COMMAND ----------

KNA1.createOrReplaceTempView("KNA1")
VBAP.createOrReplaceTempView("VBAP")
VBRP.createOrReplaceTempView("VBRP")
VBFA.createOrReplaceTempView("VBFA")
VBRK.createOrReplaceTempView("VBRK")
VBKD.createOrReplaceTempView("VBKD")
df_konv_Filtetred.createOrReplaceTempView("KONV")
KNVP.createOrReplaceTempView("KNVP")
mrd_material.createOrReplaceTempView("mrd_material")
t001w_cc_tp.createOrReplaceTempView("t001w_cc_tp")
df_vbak_Filtered.createOrReplaceTempView("VBAK")

# COMMAND ----------

# tesing code
tc_sql = spark.sql("""select 
VBAK.VBELN as Contract_No,
vbap.vbeln as Contract_no2,
--VBAK.KTEXT as Contract_Description,
VBAK.ERDAT as Creation_Date,
VBAK.GUEEN as Valid_To,
VBAK.GUEBG as Valid_From,
VBAK.AUART as Sales_Doc_Type,
VBAK.KUNNR as Sold_To,
VBAK.VSBED as Shipping_Condition_tbu,
VBAP.MATNR as Material_No,
VBAP.VOLUM as Target_Quantity,
VBAP.OID_SHIP as SHIP_TO,
vbap.werks_1 as Plant_code,
vbfa.vbeln as Delivery_Number,
case when (vbkd.BSARK_E is null and vbak.AUART = 'ZCRM') then 'TRRM'
	         when (vbkd.BSARK_E is null or length(vbkd.BSARK_E) = 0) and vbak.AUART != 'ZCRM' and konv.KSCHL = 'TERM' then 'TTFD'
			 when (vbkd.BSARK_E is null or length(vbkd.BSARK_E) = 0) and vbak.AUART != 'ZCRM' and konv.KSCHL = 'SPOT' then 'TSFP'
			 when (vbkd.BSARK_E is null or length(vbkd.BSARK_E) = 0) and vbak.AUART != 'ZCRM' and konv.KSCHL = 'TERMMAN' then 'TSFP'
			 when (vbkd.BSARK_E is null or length(vbkd.BSARK_E) = 0) and vbak.AUART != 'ZCRM' then 'TTFD'
			 else vbkd.BSARK_E end as BSARK_E,
case when vbkd.BSARK_E is null or length(vbkd.BSARK_E) = 0 then 'yes' end as PoT_manuaupdate,
case when vbap.werks_1 = 'D333' then 'D361' else vbap.werks_1 end as Plant,
VBAP.POSNR as Contract_Item,
KONV.KSCHL as Pricing_Scheme,
KNA11.NAME1 as SHIP_TO_NAME,
kna1.NAME1 as SOLD_TO_NAME,
t001w.Plant_Name,
mm.PROD_GROUP_DESC as Product_Group_Description
from vbak vbak
left join vbap vbap
on vbak.vbeln = vbap.vbeln
left join vbfa vbfa
on vbak.vbeln = vbfa.vbelv
left join KNVP knvp
on vbak.kunnr = knvp.kunnr
left join kna1 kna1
on vbak.kunnr = kna1.kunnr
left join kna1 kna11
on VBAP.oid_ship = kna11.kunnr
left join vbkd vbkd
on vbak.vbeln = vbkd.vbeln
left join vbrp vbrp
on vbak.vbeln = vbrp.vbeln
left join konv konv
on vbak.knumv = konv.knumv
left join mrd_material mm
on substring(vbap.matnr,10,length(vbap.matnr)) = mm.matnr
left join t001w_cc_tp t001w
on vbap.werks_1 = t001w.plant""")

# COMMAND ----------

tc_sql = tc_sql.dropDuplicates()

# COMMAND ----------

tc_sql.display()

# COMMAND ----------

Write_UnHarm_Path = '/mnt/ADLS'+UnHarm_Base_Folder+CDB_Table_Name
print(Write_UnHarm_Path)
WriteFile_UnHarm(CDB_Raw,Write_UnHarm_Path)
