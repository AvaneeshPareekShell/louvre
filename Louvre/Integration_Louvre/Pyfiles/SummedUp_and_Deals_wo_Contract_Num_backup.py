# Databricks notebook source
# MAGIC %run /Louvre/Generic_Functions/connectionConfig/ADLSConnection

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/properties

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/SQLConnector

# COMMAND ----------

import sys
from pyspark.sql import SparkSession, SQLContext
from time import gmtime, strftime, time
from pyspark.sql.functions import col,abs, to_timestamp, date_format,coalesce,regexp_replace,substring,trim,countDistinct
from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date,expr, row_number, concat_ws, current_timestamp, month, round, ceil, floor
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import count, avg,sum, upper
from pyspark.sql.functions import expr, first, last
from pyspark.sql.functions import to_date
from pyspark.sql.functions import concat
from pyspark.sql.functions import when,length
from pyspark.sql.functions import substring
from pyspark.sql.functions import UserDefinedFunction
import datetime
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

spark = SparkSession\
 .builder\
 .appName('Louvre')\
 .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions",'20')

# COMMAND ----------

CDB_Plan_Vol_Price_Period_2022 = readLouvreDbTable(spark,"louvre_curated.Transformed_CDB_Data_2022")

# COMMAND ----------

CDB_Plan_Vol_Price_Period_2023 = readLouvreDbTable(spark,"louvre_curated.Transformed_CDB_Data_2023")

# COMMAND ----------

CDB_Plan_Vol_Price_Period = CDB_Plan_Vol_Price_Period_2022.unionAll(CDB_Plan_Vol_Price_Period_2023)

# COMMAND ----------

#CDB_Plan_Vol_Price_Period = readLouvreDbTable(spark,"louvre_curated.Transformed_CDB_Data_2022")

# COMMAND ----------

#CDB_Filter = Transformed_CDB_Data.withColumn("Row_number",row_number().over(Window.partitionBy("material_number","ship_to_number","sold_to_number","supply_point","Term_Plan_Period").orderBy(Transformed_CDB_Data.Plan_m3.desc())))
#CDB_Plan_Vol_Price_Period = CDB_Filter.filter((CDB_Filter.Row_number == 1) & (CDB_Filter.Plan_m3 != 0))

# COMMAND ----------

# MAGIC %run /Louvre/Integration_Louvre/Pyfiles/Summed_Up_Raw

# COMMAND ----------

#source_filter = Summed_Up_Raw.select('ZMENG','ktext','ERDAT','Pricing_Scheme','AUART','OID_SHIP','kunnr','sold_to_name','BSARK_E','GUEEN','GUEBG','bukrs','MATNR','VBAP_WERKS','lifting_in_KG','lifting_in_L15','VSBED','OIHANTYP','POSNR','aedat','MBLNR','OIVBELN','abgru','ZIEME','Ship_To_Name','sold_to_name','vbeln','lifting_in_to','lifting_in_cbm','VBAP_bukrs','Contract_Start_Period','Contract_End_Period').dropDuplicates()
source_filter = Summed_Up_Raw.select('ZMENG','ktext','ERDAT','Pricing_Scheme','AUART','OID_SHIP','kunnr','BSARK_E','GUEEN','GUEBG','bukrs','MATNR','VBAP_WERKS','lifting_in_KG','lifting_in_L15','VSBED','OIHANTYP','POSNR','aedat','MBLNR','OIVBELN','abgru','ZIEME','vbeln','lifting_in_to','lifting_in_cbm','VBAP_bukrs','Contract_Start_Period','Contract_End_Period','plant_name').dropDuplicates()

# COMMAND ----------

#source.createOrReplaceTempView("source")
CDB_Plan_Vol_Price_Period.createOrReplaceTempView("CDB_Plan_Vol_Price_Period")

# COMMAND ----------

# adding Contract_Start_Period and Contract_End_Period
#source_add_Fields = source_filter.withColumn('Contract_Start_Period',concat(substring("GUEBG",0,4),lit('-'),substring("GUEBG",5,2))).\
#                          withColumn('Contract_End_Period',concat(substring("GUEEN",0,4),lit('-'),substring("GUEEN",5,2)))
source_filter.createOrReplaceTempView("source")

# COMMAND ----------

source_CDB_join = spark.sql("""select source.ZMENG as Target_Quantity,
source.ktext as Contract_Description,
source.vbeln as Contract_No,
source.ERDAT as Creation_Date,
source.Pricing_Scheme,
source.AUART as Sales_Doc_Type,
case when source.OID_SHIP is null then CDB.ship_to_number else source.OID_SHIP end as ship_to_number,
case when source.kunnr is null then CDB.sold_to_number else source.kunnr end as sold_to_number,
--case when source.sold_to_name is null then CDB.sold_to_name else source.sold_to_name end as sold_to_name,
--source.Ship_To_Name,
source.ZIEME as UOM,
source.BSARK_E as Purchase_Order_Type,
source.GUEEN as Valid_To,
source.GUEBG as Valid_From,
source.VBAP_bukrs as Company_Code, --need to check
case when source.MATNR is null then CDB.material_number else source.MATNR end as material_number,
case when source.VBAP_WERKS is null then CDB.Supply_Point else source.VBAP_WERKS end as Plant_code,
source.lifting_in_KG,
source.lifting_in_L15, --need to pick from sap_hana
source.VSBED as Shipping_Condition_tbu,
source.OIHANTYP as Handling_Type_Description,
source.POSNR as Contract_Item,
source.aedat as Last_Modefied_Date,
source.Contract_Start_Period,
source.Contract_End_Period,
source.MBLNR as Material_Document_number, --need to check
source.OIVBELN as Delivery_Number,
source.lifting_in_to,
source.lifting_in_cbm,
source.plant_name,
CDB.Agreemt_Period,
CDB.Plan_m3,
CDB.Plan_to,
CDB.Density,
CDB.Depot_Property,
CDB.FED_Contract,
CDB.MoT,
CDB.Pricing_Period,
CDB.Term_Plan_Period,
CDB.Price_Basis,
CDB.Supply_Point
from source source full join CDB_Plan_Vol_Price_Period CDB on substring(source.OID_SHIP,3,length(source.OID_SHIP))  =  CDB.ship_to_number and substring(source.MATNR,10,length(source.MATNR)) = CDB.material_number and source.VBAP_WERKS = CDB.supply_point and source.Contract_Start_Period = CDB.Term_Plan_Period""")

# COMMAND ----------

#source_CDB_join_dp = source_CDB_join.dropDuplicates()

# COMMAND ----------

#source_CDB_join.filter(source_CDB_join.Contract_No == '0322915990').display()

# COMMAND ----------

# Term Overlifts
source_CDB_join_TO = source_CDB_join.withColumn("Purchase_Order_Type", when((source_CDB_join.Purchase_Order_Type.isin('TTFD')) & (source_CDB_join.Shipping_Condition_tbu.isin('10')) & (source_CDB_join.Pricing_Scheme.isin('TERMMAN')) & (source_CDB_join.Creation_Date >= source_CDB_join.Valid_To), 'TOL_TERM').when((source_CDB_join.Purchase_Order_Type.isin('TTTT') & source_CDB_join.Shipping_Condition_tbu.isin('10') & source_CDB_join.Pricing_Scheme.isin('TERMMAN') & (source_CDB_join.Creation_Date >= source_CDB_join.Valid_To)),'TOL_TRIGGER').otherwise(source_CDB_join.Purchase_Order_Type))

# COMMAND ----------

source_CDB_join_RLO = source_CDB_join_TO.withColumn("ship_to_number",sf.regexp_replace('ship_to_number', r'^[0]*', '')).withColumn("sold_to_number",sf.regexp_replace('sold_to_number', r'^[0]*', '')).withColumn("material_number",sf.regexp_replace('material_number', r'^[0]*', '')).withColumn("Contract_No",sf.regexp_replace('Contract_No', r'^[0]*', ''))

# COMMAND ----------

KNVP = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.KNVP").select('kunn2','kunnr').dropDuplicates().withColumn("kunnr",sf.regexp_replace('kunnr', r'^[0]*', ''))
KNA1 = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.KNA1").select('NAME1','kunnr').dropDuplicates()
KNVV = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.KNVV").select('VKGRP','kunnr','VKORG').withColumn("kunnr",sf.regexp_replace('kunnr', r'^[0]*', '')).dropDuplicates()

# COMMAND ----------

li_vkorg = ["DE01","AT01"]
KNVP = KNVP.filter(KNVP.kunn2.isNotNull())
KNVV_Order = KNVV.filter((KNVV.VKGRP.isNotNull()) & (KNVV.VKORG.isin(li_vkorg))).withColumn("Row_number",row_number().over(Window.partitionBy("kunnr","VKORG").orderBy(KNVV.VKGRP.asc())))
KNVV_filter = KNVV_Order.filter(KNVV_Order.Row_number == 1).drop("VKORG").drop("Row_number")

# COMMAND ----------

si_kam_mapping = readLouvreDbTable(spark,"louvre_raw.SI_KAM_Mapping").select("Customer","Partner_Sales_Group","Sales group SP","Partner").withColumnRenamed("Sales group SP","Sales_group_SP").withColumn("Partner",sf.regexp_replace('Partner', r'^[0]*', '')).withColumn("Customer",sf.regexp_replace('Customer', r'^[0]*', '')).dropDuplicates()

# COMMAND ----------

# Creating SI field
#source_CDB_join_SI = source_CDB_join_RLO.join(si_kam_mapping,source_CDB_join_RLO["ship_to_number"] == si_kam_mapping["Partner"],"left").withColumnRenamed("Partner_Sales_Group","SI").drop("Customer").drop("Partner").drop("Sales_group_SP")
source_CDB_join_SI = source_CDB_join_RLO.join(KNVV_filter,source_CDB_join_RLO["ship_to_number"] == KNVV_filter["KUNNR"],"left").withColumnRenamed("VKGRP","SI").drop(KNVV_filter.kunnr)

# COMMAND ----------

# Creating KAM Field
#source_CDB_join_KAM = source_CDB_join_SI.join(si_kam_mapping,source_CDB_join_SI["sold_to_number"] == si_kam_mapping["Partner"],"left").withColumnRenamed("Partner_Sales_Group","KAM").drop("Customer").drop("Partner").drop("Sales_group_SP")
source_CDB_join_KAM = source_CDB_join_SI.join(KNVV_filter,source_CDB_join_SI["sold_to_number"] == KNVV_filter["kunnr"],"left").withColumnRenamed("VKGRP","KAM").drop(KNVV_filter.kunnr)

# COMMAND ----------

source_CDB_join_SI_Mapping = source_CDB_join_KAM.join(si_kam_mapping,source_CDB_join_KAM["ship_to_number"] == si_kam_mapping["Partner"],"left").withColumn("SI", when(source_CDB_join_KAM.SI.isNull(),si_kam_mapping.Partner_Sales_Group).otherwise(source_CDB_join_KAM.SI)).drop("Customer").drop("Partner").drop("Sales_group_SP").drop("Partner_Sales_Group")

# COMMAND ----------

source_CDB_join_KAM_Mapping = source_CDB_join_SI_Mapping.join(si_kam_mapping,source_CDB_join_SI_Mapping["sold_to_number"] == si_kam_mapping["Partner"],"left").withColumn("KAM", when(source_CDB_join_SI_Mapping.KAM.isNull(),si_kam_mapping.Partner_Sales_Group).otherwise(source_CDB_join_SI_Mapping.KAM)).drop("Customer").drop("Partner").drop("Sales_group_SP").drop("Partner_Sales_Group")

# COMMAND ----------

#join KNVP to bring KUNN2 for creating National Account number Field
source_CDB_join_kunn2 = source_CDB_join_KAM_Mapping.join(KNVP,source_CDB_join_KAM_Mapping.sold_to_number == KNVP.kunnr,'left').drop(KNVP.kunnr)

# COMMAND ----------

#creating National Account Name Field
source_CDB_join_NAN = source_CDB_join_kunn2.join(KNA1,source_CDB_join_kunn2.kunn2 == KNA1.kunnr,'left').withColumnRenamed("NAME1","National_Account_Name").drop("kunn2")

# COMMAND ----------

#source_CDB_join_NAN_Null = source_CDB_join_NAN.withColumn("National_Account_Name", when(source_CDB_join_NAN.National_Account_Name.isNull(),source_CDB_join_NAN.sold_to_name).otherwise(source_CDB_join_NAN.National_Account_Name))

# COMMAND ----------

source_CDB_join_NAN.createOrReplaceTempView("source_CDB_join_NAN")

# COMMAND ----------

# Applying Tranformation logic on Pricing_period based on Valid From
source_CDB_Pricing_Period = spark.sql("""select *,
case when source_CDB_join_NAN.Pricing_Period = 'Half-Month' and source_CDB_join_NAN.Valid_From in ("20220101","20220201","20220301","20220401","20220501","20220601","20220701","20220801","20220901","20221001","20221101","20221201","20230101","20230201","20230301","20230401","20230501","20230601","20230701","20230801","20230901","20231001","20231101","20231201") then "H1"
            when source_CDB_join_NAN.Pricing_Period = 'Half-Month' and source_CDB_join_NAN.Valid_From in ("20220116","20220216","20220316","20220416","20220516","20220616","20220716","20220816","20220916","20221016","20221116","20221216","20230116","20230216","20230316","20230416","20230516","20230616","20230716","20230816","20230916","20231016","20231116","20231216") then "H2"
	 when source_CDB_join_NAN.Pricing_Period = 'Decade' and source_CDB_join_NAN.Valid_From in ("20220101","20220201","20220301","20220401","20220501","20220601","20220701","20220801","20220901","20221001","20221101","20221201","20230101","20230201","20230301","20230401","20230501","20230601","20230701","20230801","20230901","20231001","20231101","20231201") then "D1"
	 when source_CDB_join_NAN.Pricing_Period = 'Decade' and source_CDB_join_NAN.Valid_From  in ("20220111","20220211","20220311","20220411","20220511","20220611","20220711","20220811","20220911","20221011","20221111","20221211","20230111","20230211","20230311","20230411","20230511","20230611","20230711","20230811","20230911","20231011","20231111","20231211") then "D2"
	 when source_CDB_join_NAN.Pricing_Period = 'Decade' and (source_CDB_join_NAN.Valid_From is null or source_CDB_join_NAN.Valid_From  not in ("20220111","20220211","20220311","20220411","20220511","20220611","20220711","20220811","20220911","20221011","20221111","20221211","20220101","20220201","20220301","20220401","20220501","20220601","20220701","20220801","20220901","20221001","20221101","20221201","20230111","20230211","20230311","20230411","20230511","20230611","20230711","20230811","20230911","20231011","20231111","20231211","20230101","20230201","20230301","20230401","20230501","20230601","20230701","20230801","20230901","20231001","20231101","20231201")) then "D3"   
     else source_CDB_join_NAN.Pricing_Period end as Pricing_Period1,
     case when source_CDB_join_NAN.Pricing_Period = 'Half-Month' then source_CDB_join_NAN.Plan_m3/2
          when source_CDB_join_NAN.Pricing_Period = 'Decade' then source_CDB_join_NAN.Plan_m3/3
          else Plan_m3 end as plan_m31
 from source_CDB_join_NAN""")

# COMMAND ----------

source_CDB_Pricing_Period = source_CDB_Pricing_Period.drop("Pricing_Period","plan_m3").withColumnRenamed("Pricing_Period1","Pricing_Period").withColumnRenamed("plan_m31","plan_m3")

# COMMAND ----------

#Null Liftings
#source_CDB_null_lift_unique = source_CDB_Pricing_Period.drop_duplicates(("SI","Company_Code","Contract_End_period","Contract_Item","Contract_No","Last_Modefied_Date","Purchase_Order_Type","lifting_in_to","Material_Document_number","Plan_m3"))
source_CDB_null_lift_unique_RN = source_CDB_Pricing_Period.withColumn("Row_number",row_number().over(Window.partitionBy("SI",
"Company_Code",
"Contract_End_period",
"Contract_Item",
"Contract_No",
"Last_Modefied_Date",
"Purchase_Order_Type",
"lifting_in_to",
"Material_Document_number",
"Plan_m3").orderBy(source_CDB_Pricing_Period.FED_Contract.desc())))
source_CDB_null_lift_unique = source_CDB_null_lift_unique_RN.filter(source_CDB_null_lift_unique_RN.Row_number == 1).drop(source_CDB_null_lift_unique_RN.Row_number)
source_CDB_null_lift_unique = source_CDB_null_lift_unique.select(sorted(source_CDB_null_lift_unique.columns))
source_CDB_null_lift_non_unique = source_CDB_Pricing_Period.exceptAll(source_CDB_null_lift_unique)
source_CDB_null_lift_non_unique_filter1 = source_CDB_null_lift_non_unique.filter(source_CDB_null_lift_non_unique.lifting_in_KG.isNull() & source_CDB_null_lift_non_unique.lifting_in_L15.isNull())
#source_CDB_null_lift_non_unique_filter = source_CDB_null_lift_non_unique_filter.withColumn("plant_code1",when(source_CDB_null_lift_non_unique_filter.Plant_code.isNull(), #source_CDB_null_lift_non_unique_filter.Supply_Point).otherwise(source_CDB_null_lift_non_unique_filter.Plant_code))
#source_CDB_null_lift_non_unique_filter = source_CDB_null_lift_non_unique_filter.drop("plant_code").withColumnRenamed("plant_code1","Plant_code")
source_CDB_null_lift_non_unique_filter = source_CDB_null_lift_non_unique_filter1.select(sorted(source_CDB_null_lift_non_unique_filter1.columns))

# COMMAND ----------

#source_CDB_null_lift_unique.filter(source_CDB_null_lift_unique.Contract_No == '322883789').display()

# COMMAND ----------

#Null Liftings on delivery number
#source_CDB_null_delivery_unique = source_CDB_null_lift_unique.drop_duplicates(("Delivery_Number","lifting_in_to","lifting_in_cbm"))
source_CDB_null_delivery_RN = source_CDB_null_lift_unique.withColumn("Row_number",row_number().over(Window.partitionBy("Delivery_Number","lifting_in_to","lifting_in_cbm").orderBy(source_CDB_null_lift_unique.FED_Contract.asc_nulls_last())))
source_CDB_null_delivery_unique = source_CDB_null_delivery_RN.filter(source_CDB_null_delivery_RN.Row_number == 1).drop(source_CDB_null_delivery_RN.Row_number)
source_CDB_null_delivery_non_unique = source_CDB_null_lift_unique.exceptAll(source_CDB_null_delivery_unique)
source_CDB_null_delivery_non_unique_filter = source_CDB_null_delivery_non_unique.filter(source_CDB_null_delivery_non_unique.lifting_in_KG.isNull() & source_CDB_null_delivery_non_unique.lifting_in_L15.isNull())

# COMMAND ----------

#source_CDB_null_delivery_unique.filter(source_CDB_null_delivery_unique.Contract_No == '322883789').display()

# COMMAND ----------

# make union of all null lifiting data frames
source_CDB_null_liftings = source_CDB_null_delivery_unique.unionAll(source_CDB_null_lift_non_unique_filter).unionAll(source_CDB_null_delivery_non_unique_filter)

# COMMAND ----------

#source_CDB_null_liftings.filter(source_CDB_null_liftings.Contract_No == '322883789').display()

# COMMAND ----------

df_Material_Customer_Mapping = readLouvreDbTable(spark,"louvre_raw.material_mapping").select("Product SubGroup","Material Number","Subgrade").withColumnRenamed("Product SubGroup","Product_SubGroup").withColumnRenamed("Material Number","Material_Number")

# COMMAND ----------

# creating Mapping tables
#df_Material_Customer_Mapping = readLouvreDbTable(spark,"louvre_raw.material_mapping").withColumnRenamed("Product SubGroup","Product_SubGroup").withColumnRenamed("Material Number","Material_Number")
#df_Invoice_Type_Mapping_Excise_Duty = readLouvreDbTable(spark,"louvre_raw.excise_duty")
#df_Invoice_Type_Mapping = readLouvreDbTable(spark,"louvre_raw.shipping_condition")

# COMMAND ----------

# renaming the fields
#df_Material_Customer_Mapping = df_Material_Customer_Mapping.withColumnRenamed("Product SubGroup","Product_SubGroup")\
                            # .withColumnRenamed("Material Number","Material_Number")
#df_Invoice_Type_Mapping_Excise_Duty = df_Invoice_Type_Mapping_Excise_Duty.withColumnRenamed("Handling Type","Handling_Type")\
                                  # .withColumnRenamed("Handling Type Description","Handling_Type_Description")
#df_Invoice_Type_Mapping = df_Invoice_Type_Mapping.withColumnRenamed("Shipping Condition","Shipping_Condition")\
                                  # .withColumnRenamed("Shipping Condition Description","Shipping_Condition_Description")

# COMMAND ----------

#df_Invoice_Type_Mapping.createOrReplaceTempView("Invoice_Type_Mapping")
#df_Invoice_Type_Mapping_Excise_Duty.createOrReplaceTempView("Invoice_Type_Mapping_Excise_Duty")
#df_Material_Customer_Mapping.createOrReplaceTempView("Material_Customer_Mapping")

# COMMAND ----------

#source_CDB_null_liftings.createOrReplaceTempView("source_CDB_null_liftings")

# COMMAND ----------

#picking grade from material mapping
#source_CDB_join_gr = spark.sql("""select source_CDB_null_liftings.*,MCM.Product_SubGroup as grade from source_CDB_null_liftings source_CDB_null_liftings
#left join Invoice_Type_Mapping ITM on source_CDB_null_liftings.Shipping_Condition_tbu = ITM.Shipping_Condition
#left join Invoice_Type_Mapping_Excise_Duty ITMED on source_CDB_null_liftings.Handling_Type_Description = ITMED.Handling_Type
#left join Material_Customer_Mapping MCM on source_CDB_null_liftings.material_number = MCM.Material_Number""")

# COMMAND ----------

source_CDB_join_gr = source_CDB_null_liftings.join(df_Material_Customer_Mapping, source_CDB_null_liftings.material_number == df_Material_Customer_Mapping.Material_Number,'left').withColumnRenamed("Product_SubGroup", "Grade").drop(df_Material_Customer_Mapping.Material_Number)

# COMMAND ----------

#source_CDB_join_gr.filter(source_CDB_join_gr.Contract_No == '322869712').display(

# COMMAND ----------

group_by_Cal = source_CDB_join_gr.groupBy("Agreemt_Period","Company_Code","Contract_Description","Contract_End_Period","Contract_No","Contract_Item","Purchase_Order_Type","Contract_Start_Period","Creation_Date","Density","Depot_Property","FED_Contract","Grade","KAM","Last_Modefied_Date","material_number","MoT","National_Account_Name","Plant_code","Price_Basis","Pricing_Period","Pricing_Scheme","Sales_Doc_Type","ship_to_number","SI","sold_to_number","Term_Plan_Period","UOM","Valid_From","Valid_To","Shipping_Condition_tbu","plant_name","Subgrade") \
.agg(avg("Target_Quantity").alias("Avg_Target_Quantity"), avg("Plan_m3").alias("Avg_Plan_m3"),  avg("Plan_to").alias("Avg_Plan_to"), sum("lifting_in_L15").alias("sum_lifting_in_L15"), sum("lifting_in_KG").alias("sum_lifting_in_KG"),sum("lifting_in_to").alias("sum_lifting_in_to"),sum("lifting_in_cbm").alias("sum_lifting_in_cbm"))

# COMMAND ----------

#group_by_Cal.filter(group_by_Cal.Contract_No.isin('322920674','322957156')).display()

# COMMAND ----------

#group_by.filter(group_by.Contract_No == '322858554').display()

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1(sold to name)
df_mergerd_kna1_soldname = group_by_Cal.join(KNA1_Filtered,group_by_Cal["sold_to_number"] ==  KNA1_Filtered["kunnr"],"left").drop(KNA1_Filtered.kunnr).withColumnRenamed("name1","Sold_To_Name")

# COMMAND ----------

# Join VBAK,VBAP,VBFA,KNA1(ship to name)
df_mergerd_kna1_shipname = df_mergerd_kna1_soldname.join(KNA1_Filtered,df_mergerd_kna1_soldname["ship_to_number"] ==  KNA1_Filtered["kunnr"],"left").drop(KNA1_Filtered.kunnr).withColumnRenamed("name1","Ship_To_Name")

# COMMAND ----------

df_mergerd_kna1_shipname = df_mergerd_kna1_shipname.withColumn("National_Account_Name", when(df_mergerd_kna1_shipname.National_Account_Name.isNull(),df_mergerd_kna1_shipname.Sold_To_Name).otherwise(df_mergerd_kna1_shipname.National_Account_Name))

# COMMAND ----------

group_by = df_mergerd_kna1_shipname.withColumn("sum_lifting_in_L15",abs(df_mergerd_kna1_shipname.sum_lifting_in_L15)).withColumn("sum_lifting_in_KG",abs(df_mergerd_kna1_shipname.sum_lifting_in_KG)).withColumn("sum_lifting_in_to",abs(df_mergerd_kna1_shipname.sum_lifting_in_to)).withColumn("sum_lifting_in_cbm",abs(df_mergerd_kna1_shipname.sum_lifting_in_cbm))

# COMMAND ----------

# calculating Trigger Contract Num Plan
group_by_filter1 = group_by.filter((group_by.Pricing_Period.isin('Trigger') & ~group_by.Purchase_Order_Type.isin('TTTM')))
group_by_filter2 = group_by.filter((~group_by.Pricing_Period.isin('Trigger') &  group_by.MoT.isin('ITT','Barge','Rail')))
group_by_filter =  group_by_filter1.union(group_by_filter2)                            
group_by_TCNP = group_by_filter.groupBy("Contract_Start_Period","ship_to_number","Plant_code","material_number","MoT") \
.agg(count("Contract_No").alias("count"), avg("Avg_Plan_m3").alias("Avg_Avg_Plan_m3"), avg("Avg_Plan_to").alias("Avg_Avg_Plan_to"))
group_by_TCNP = group_by_TCNP.withColumn("Trigger_Contract_Num_Plan", group_by_TCNP["Avg_Avg_Plan_m3"]/group_by_TCNP["Count"]) \
                             .withColumn("Trigger_Contract_Num_Plan_To", group_by_TCNP["Avg_Avg_Plan_to"]/group_by_TCNP["Count"])

# COMMAND ----------

# Adding  Trigger Contract Num Plan
group_by_TCNP_jn = group_by.join(group_by_TCNP,(group_by["ship_to_number"] == group_by_TCNP["ship_to_number"]) & (group_by["Plant_code"] == group_by_TCNP["Plant_code"]) & (group_by["material_number"] == group_by_TCNP["material_number"]) & (group_by["Contract_Start_Period"] == group_by_TCNP["Contract_Start_Period"]),'left').select(group_by["*"],group_by_TCNP["Trigger_Contract_Num_Plan"],group_by_TCNP["Trigger_Contract_Num_Plan_To"])

# COMMAND ----------

#Trigger special average plan m3 transformation
group_by_Trigger_avg = group_by_TCNP_jn.withColumn("Avg_Plan_m3", when((group_by_TCNP_jn.Pricing_Period == 'Trigger') | (group_by_TCNP_jn.MoT.isin('ITT','Barge','Rail')),group_by_TCNP_jn.Trigger_Contract_Num_Plan).otherwise(group_by_TCNP_jn.Avg_Plan_m3))\
                                       .withColumn("Avg_Plan_to", when((group_by_TCNP_jn.Pricing_Period == 'Trigger') | (group_by_TCNP_jn.MoT.isin('ITT','Barge','Rail')),group_by_TCNP_jn.Trigger_Contract_Num_Plan_To).otherwise(group_by_TCNP_jn.Avg_Plan_to))

# COMMAND ----------

# Missing POT Updates
group_by_TCNP_POT = group_by_Trigger_avg.withColumn("Purchase_Order_Type",when((group_by_Trigger_avg.Purchase_Order_Type.isNull()) & \
                                                                          (group_by_Trigger_avg.Pricing_Period != 'Trigger'),'TTFD').when((group_by_Trigger_avg.Purchase_Order_Type.isNull()) & (group_by_Trigger_avg.Pricing_Period == 'Trigger'),'TTTT').otherwise(group_by_Trigger_avg.Purchase_Order_Type))

# COMMAND ----------

group_by_TCNP_POT.createOrReplaceTempView("group_by_TCNP_jn")

# COMMAND ----------

# Adding Target_Qty_to, Target_Qty_m3 and Tolerance_m15
group_by_TCNP_jn_Target_Quantity_to_m3 =  spark.sql("""select *,case when group_by_TCNP_jn.UOM = 'L15' then group_by_TCNP_jn.Avg_Target_Quantity
     when UOM = 'TO' and Grade = 'Mogas' then group_by_TCNP_jn.Avg_Target_Quantity
	 when group_by_TCNP_jn.UOM = 'TO' and Grade = 'GtL' then group_by_TCNP_jn.Avg_Target_Quantity
	 when group_by_TCNP_jn.UOM = 'TO' and Grade != 'Mogas' and Grade != 'GtL'  then group_by_TCNP_jn.Avg_Target_Quantity
     when group_by_TCNP_jn.UOM != 'L15' and group_by_TCNP_jn.UOM != 'TO' and Grade = 'Mogas' then group_by_TCNP_jn.Avg_Target_Quantity*0.755
	 when group_by_TCNP_jn.UOM != 'L15' and group_by_TCNP_jn.UOM != 'TO' and Grade = 'GtL' then group_by_TCNP_jn.Avg_Target_Quantity*0.779
	 when group_by_TCNP_jn.UOM != 'L15' and group_by_TCNP_jn.UOM != 'TO' and Grade != 'Mogas' and Grade != 'GtL' then group_by_TCNP_jn.Avg_Target_Quantity*0.845 end as Target_Qty_to,
case when group_by_TCNP_jn.UOM = 'L15' then group_by_TCNP_jn.Avg_Target_Quantity/1000
     when UOM = 'TO' and Grade = 'Mogas' then group_by_TCNP_jn.Avg_Target_Quantity/0.755
	 when group_by_TCNP_jn.UOM = 'TO' and Grade = 'GtL' then group_by_TCNP_jn.Avg_Target_Quantity/0.779
	 when group_by_TCNP_jn.UOM = 'TO' and Grade != 'Mogas' and Grade != 'GtL'  then group_by_TCNP_jn.Avg_Target_Quantity/0.845
     when group_by_TCNP_jn.UOM != 'L15' and group_by_TCNP_jn.UOM != 'TO' and Grade = 'Mogas' then group_by_TCNP_jn.Avg_Target_Quantity
	 when group_by_TCNP_jn.UOM != 'L15' and group_by_TCNP_jn.UOM != 'TO' and Grade = 'GtL' then group_by_TCNP_jn.Avg_Target_Quantity
	 when group_by_TCNP_jn.UOM != 'L15' and group_by_TCNP_jn.UOM != 'TO' and Grade != 'Mogas' and Grade != 'GtL' then group_by_TCNP_jn.Avg_Target_Quantity end as Target_Qty_m3 from group_by_TCNP_jn""")

# COMMAND ----------

group_by_TCNP_jn_Target_Quantity_to_m3.createOrReplaceTempView("group_by_TCNP_jn_Target_Quantity_to_m3")

# COMMAND ----------

group_by_TCNP_jn_Target_Quantity =  spark.sql("""select *,case when group_by_TCNP_jn_Target_Quantity_to_m3.Purchase_Order_Type != 'TSFP' and group_by_TCNP_jn_Target_Quantity_to_m3.Purchase_Order_Type = 'TRRM' then group_by_TCNP_jn_Target_Quantity_to_m3.Avg_Target_Quantity*0.00
     when group_by_TCNP_jn_Target_Quantity_to_m3.Purchase_Order_Type != 'TSFP' and group_by_TCNP_jn_Target_Quantity_to_m3.Purchase_Order_Type != 'TRRM' then group_by_TCNP_jn_Target_Quantity_to_m3.Target_Qty_m3*0.05
	 when group_by_TCNP_jn_Target_Quantity_to_m3.Purchase_Order_Type = 'TSFP' and group_by_TCNP_jn_Target_Quantity_to_m3.Avg_Target_Quantity > 300 then '15'
	 when group_by_TCNP_jn_Target_Quantity_to_m3.Purchase_Order_Type = 'TSFP' and group_by_TCNP_jn_Target_Quantity_to_m3.Avg_Target_Quantity <= 300 and group_by_TCNP_jn_Target_Quantity_to_m3.Avg_Target_Quantity > 100 then '10'
	 when group_by_TCNP_jn_Target_Quantity_to_m3.Purchase_Order_Type = 'TSFP' and group_by_TCNP_jn_Target_Quantity_to_m3.Avg_Target_Quantity <= 100 then '5' end as Tolerance_m15
     from group_by_TCNP_jn_Target_Quantity_to_m3""")

# COMMAND ----------

Mount_Path = LouvreConfig["dataSets"]["Louvre_ADLS"]["Mount_Path"]
SF_UnHarm_Path =  Mount_Path+LouvreConfig["dataSets"]["l_ADLS_Loc"]["RSO_Loc"]["unharm"]+"SALESFORCE_SALES_ORDER_PRICE_AUDIT_REPORT"+'/'
df_price_audit_report = ReadFile_UnHarm(spark,SF_UnHarm_Path).select(['SAP_Contract_Number','SHT_Number','Material_No','Plant_No','Volume_CBM']).dropDuplicates().withColumn("SAP_Contract_Number", sf.regexp_replace('SAP_Contract_Number', r'^[0]*', '')).withColumn("Material_No", sf.regexp_replace('Material_No', r'^[0]*', ''))
#writeLouvreDbTable(df_price_audit_report,"louvre_raw.price_audit_report")
group_by_TCNP_jn_Target_Quantity_SF = group_by_TCNP_jn_Target_Quantity.join(df_price_audit_report, (group_by_TCNP_jn_Target_Quantity["Contract_No"] == df_price_audit_report["SAP_Contract_Number"]) & (group_by_TCNP_jn_Target_Quantity["material_number"] == df_price_audit_report["Material_No"]) & (group_by_TCNP_jn_Target_Quantity["Plant_code"] == df_price_audit_report["Plant_No"]),'left').withColumn("Volume_CBM", when(col('Volume_CBM').isNull(),col('Target_Qty_m3')).otherwise(col('Volume_CBM'))).drop("SAP_Contract_Number","SHT_Number","Material_No","Plant_No")

# COMMAND ----------

group_by_TCNP_jn_Target_Quantity_SF.createOrReplaceTempView("group_by_TCNP_jn_Target_Quantity")

# COMMAND ----------

 # adding Erfüllt_Zu, FullCalc_Phil and applying transformations on Avg_Plan m3 and Pricing_Period
group_by_Er_Fu  = spark.sql("""select group_by_TCNP_jn_Target_Quantity.*,case when group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type in ('TSFP','TTTT','TRRM','TOL','TTTS') and  group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type in ('TSFP','TRRM','TOL') then 'spot' else group_by_TCNP_jn_Target_Quantity.Pricing_Period end as Pricing_Period1,
case when group_by_TCNP_jn_Target_Quantity.Contract_Start_Period is null then group_by_TCNP_jn_Target_Quantity.Term_Plan_Period else Contract_Start_Period end as Contract_Start_Period1,
case when group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type in ('TSFP','TTTT','TRRM','TOL','TTTS') then group_by_TCNP_jn_Target_Quantity.sum_lifting_in_cbm/group_by_TCNP_jn_Target_Quantity.Target_Qty_m3
	 when group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type not in ('TSFP','TTTT','TRRM','TOL','TTTS') then group_by_TCNP_jn_Target_Quantity.sum_lifting_in_cbm/group_by_TCNP_jn_Target_Quantity.Avg_Plan_m3
	 end as Erfullt_Zu,	 
case when group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type in ('TRRM','TOL_TERM','TOL_TRIGGER') then Target_Qty_m3
     when group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type in ('TSFP','TTTT','TTTS','TTTM','TTTI') then Volume_CBM
	 when group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type not in ('TSFP','TTTT','TRRM','TOL_TERM','TOL_TRIGGER','TTTS','TTTM','TTTI') then Avg_Plan_m3
	 end as FullCalc_Phil,
case when group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type like 'TTT%' and group_by_TCNP_jn_Target_Quantity.Purchase_Order_Type != 'TTTS' and group_by_TCNP_jn_Target_Quantity.Contract_No is not null then group_by_TCNP_jn_Target_Quantity.Trigger_Contract_Num_Plan
else group_by_TCNP_jn_Target_Quantity.Avg_Plan_m3 end as Avg_Plan_m3_1 from group_by_TCNP_jn_Target_Quantity""")

# COMMAND ----------

group_by_Er_Fu = group_by_Er_Fu.drop("Contract_Start_Period").withColumnRenamed("Contract_Start_Period1","Contract_Start_Period")

# COMMAND ----------

# replacing nulls with 1 for FullCalc_Phil
group_by_Er_Fu1 = group_by_Er_Fu.withColumn('FullCalc_Phil', when(group_by_Er_Fu.FullCalc_Phil.isNull(),1).otherwise(group_by_Er_Fu.FullCalc_Phil))


# COMMAND ----------

## applying transformations on FullCalc_Phil and renaming transformed fields
group_by_Er_Fu2 = group_by_Er_Fu1.withColumn('sum_lifting_in_cbm', when(col('sum_lifting_in_cbm').isNull(),0).otherwise(col('sum_lifting_in_cbm'))).\
withColumn('sum_lifting_in_to', when(col('sum_lifting_in_to').isNull(),0).otherwise(col('sum_lifting_in_to'))).withColumn('FullCalc_Phil', when(group_by_Er_Fu1.FullCalc_Phil < 3,group_by_Er_Fu1.FullCalc_Phil/1000).otherwise(group_by_Er_Fu1.FullCalc_Phil)). \
drop("Pricing_Period","Avg_Plan_m3"). \
withColumnRenamed("Pricing_Period1","Pricing_Period"). \
withColumnRenamed("Avg_Plan_m3_1","Avg_Plan_m3")

# COMMAND ----------

group_by_Er_Fu2.count()

# COMMAND ----------

#group_by_Er_Fu_cbm_null_negative = group_by_Er_Fu2.filter((group_by_Er_Fu2.sum_lifting_in_cbm.isNotNull()) & (trim(group_by_Er_Fu2.Depot_Property) != 'back-up') & (group_by_Er_Fu2.Avg_Plan_m3 > '5'))

# COMMAND ----------

group_by_Er_Fu_cbm_null_negative.display()

# COMMAND ----------

# remvoing records where lifiting in cbm is null and depot property in back-up
#group_by_Er_Fu_cbm_null_not = group_by_Er_Fu2.filter((group_by_Er_Fu2.sum_lifting_in_cbm.isNotNull()) & (trim(group_by_Er_Fu2.Depot_Property) != 'back-up') & (group_by_Er_Fu2.Avg_Plan_m3 > 5))
group_by_Er_Fu_cbm_null = group_by_Er_Fu2.filter((group_by_Er_Fu2.sum_lifting_in_cbm == 0) & (trim(group_by_Er_Fu2.Depot_Property) == 'back-up') & (group_by_Er_Fu2.Avg_Plan_m3 < 5))
group_by_Er_Fu_cbm_not_null = group_by_Er_Fu2.exceptAll(group_by_Er_Fu_cbm_null)

# COMMAND ----------

#group_by_Er_Fu_cbm_null_not.count()

# COMMAND ----------

#group_by_Er_Fu_cbm_not_null.filter(group_by_Er_Fu_cbm_not_null.Contract_No == '322979803').display()

# COMMAND ----------

#group_by_Er_Fu_cbm_null.filter(group_by_Er_Fu_cbm_null.Contract_No == '322979803').display()

# COMMAND ----------

#group_by_Er_Fu_cbm_not_null.filter(group_by_Er_Fu_cbm_not_null.Contract_No == '322979803').display()

# COMMAND ----------

# calculating open quantity
group_by_OQ_test = group_by_Er_Fu_cbm_not_null.withColumn('Open_Qty_m3', group_by_Er_Fu_cbm_not_null.FullCalc_Phil-group_by_Er_Fu_cbm_not_null.sum_lifting_in_cbm).withColumn('Creation_Date',to_date(concat_ws('',group_by_Er_Fu_cbm_not_null.Creation_Date.substr(1, 4), lit('-'), group_by_Er_Fu_cbm_not_null.Creation_Date.substr(5, 2), lit('-'), group_by_Er_Fu_cbm_not_null.Creation_Date.substr(7, 2)),'yyyy-MM-dd')).withColumn('Valid_From',to_date(concat_ws('',group_by_Er_Fu_cbm_not_null.Valid_From.substr(1, 4), lit('-'), group_by_Er_Fu_cbm_not_null.Valid_From.substr(5, 2), lit('-'), group_by_Er_Fu_cbm_not_null.Valid_From.substr(7, 2)),'yyyy-MM-dd')).withColumn('Valid_To',to_date(concat_ws('',group_by_Er_Fu_cbm_not_null.Valid_To.substr(1, 4), lit('-'), group_by_Er_Fu_cbm_not_null.Valid_To.substr(5, 2), lit('-'), group_by_Er_Fu_cbm_not_null.Valid_To.substr(7, 2)),'yyyy-MM-dd'))

# COMMAND ----------

group_by_OQ_test.filter((group_by_OQ_test.sum_lifting_in_cbm.isNull()) & (trim(group_by_OQ_test.Depot_Property) == 'back-up') & (group_by_OQ_test.Avg_Plan_m3 < 5)).display()

# COMMAND ----------

# removing nulls in lifiting in cbm
group_by_lift = group_by_Er_Fu_cbm_not_null.dropDuplicates().withColumn('sum_lifting_in_cbm', when(group_by_Er_Fu_cbm_not_null.sum_lifting_in_cbm.isNull(),0).otherwise(group_by_Er_Fu_cbm_not_null.sum_lifting_in_cbm)).\
withColumn('sum_lifting_in_to', when(group_by_Er_Fu_cbm_not_null.sum_lifting_in_to.isNull(),0).otherwise(group_by_Er_Fu_cbm_not_null.sum_lifting_in_to))

# COMMAND ----------

group_by_lift_test = group_by_Er_Fu_cbm_not_null.dropDuplicates()

# COMMAND ----------

group_by_lift.cache()

# COMMAND ----------

group_by_lift.filter(group_by_lift.Contract_No == '322979803').display()

# COMMAND ----------



# COMMAND ----------

# calculating open quantity
group_by_OQ = group_by_lift.withColumn('Open_Qty_m3', group_by_lift.FullCalc_Phil-group_by_lift.sum_lifting_in_cbm).withColumn('Creation_Date',to_date(concat_ws('',group_by_lift.Creation_Date.substr(1, 4), lit('-'), group_by_lift.Creation_Date.substr(5, 2), lit('-'), group_by_lift.Creation_Date.substr(7, 2)),'yyyy-MM-dd')).withColumn('Valid_From',to_date(concat_ws('',group_by_lift.Valid_From.substr(1, 4), lit('-'), group_by_lift.Valid_From.substr(5, 2), lit('-'), group_by_lift.Valid_From.substr(7, 2)),'yyyy-MM-dd')).withColumn('Valid_To',to_date(concat_ws('',group_by_lift.Valid_To.substr(1, 4), lit('-'), group_by_lift.Valid_To.substr(5, 2), lit('-'), group_by_lift.Valid_To.substr(7, 2)),'yyyy-MM-dd'))

# COMMAND ----------

#formatting date for valid-to and valid-from
df_date_formatted = group_by_OQ.withColumn('Valid_From_formatted',date_format(col('Valid_From'), "dd.MM.yyyy")) \
                                .withColumn('Valid_To_formatted', date_format(col('Valid_To'), "dd.MM.yyyy"))

#mapping MoT corresponding to shipping conditions
barge = ['30','31','40','41']
rail = ['20','21']
truck = ['10','11']
itt = ['80']
df_mot = df_date_formatted.withColumn('MoT', when(col('Shipping_Condition_tbu').isin(barge),'Barge').when(col('Shipping_Condition_tbu').isin(rail),'Rail').when(col('Shipping_Condition_tbu').isin(truck),'Truck').when(col('Shipping_Condition_tbu').isin(itt),'ITT').otherwise(''))

# COMMAND ----------

df_mot.cache()

# COMMAND ----------

#summed_up = df_mot.filter((df_mot.Contract_No.isNotNull())).dropDuplicates(['Contract_No','Plant_Code','Material_Number','Term_Plan_Period'])
summed_up_RN = df_mot.filter(df_mot.Contract_No.isNotNull()).withColumn("Row_number",row_number().over(Window.partitionBy("Contract_No","Plant_Code","Material_Number","Term_Plan_Period").orderBy(df_mot.sum_lifting_in_cbm.desc())))
summed_up = summed_up_RN.filter(summed_up_RN.Row_number == 1).drop("Row_number")

# COMMAND ----------

summed_up_key = summed_up.withColumn("KEY",concat(coalesce(col('Contract_No'),lit('')),coalesce(col('Plant_Code'),lit('')),coalesce(col('Material_Number'),lit('')),coalesce(col('Term_Plan_Period'),lit(''))))

# COMMAND ----------

summed_up_sumField = summed_up_key.groupBy(summed_up_key.columns).agg(sum(summed_up_key.FullCalc_Phil).alias("sum_FullCalc_Phil"), sum(summed_up_key.sum_lifting_in_cbm).alias("sum_sum_lifting_in_cbm"))

# COMMAND ----------

austria_SN = {'12347079','12591939'}
austria_plant_code_exclude = {'A004','A005'}
summed_up_austria_ship_to_number = summed_up_sumField.withColumn("Avg_Plan_m3", when(((summed_up_sumField.ship_to_number.isin(austria_SN)) & (~summed_up_sumField.Plant_code.isin(austria_plant_code_exclude))),summed_up_sumField.Avg_Plan_m3/2).otherwise(summed_up_sumField.Avg_Plan_m3)) \
                                .withColumn("Avg_Plan_to", when(((summed_up_sumField.ship_to_number.isin(austria_SN)) & (~summed_up_sumField.Plant_code.isin(austria_plant_code_exclude))),summed_up_sumField.Avg_Plan_to/2).otherwise(summed_up_sumField.Avg_Plan_to)) \
                                .withColumn("FullCalc_Phil", when(((summed_up_sumField.ship_to_number.isin(austria_SN)) & (~summed_up_sumField.Plant_code.isin(austria_plant_code_exclude))),summed_up_sumField.FullCalc_Phil/2).otherwise(summed_up_sumField.FullCalc_Phil))

# COMMAND ----------

summed_up_calculated_Fields = summed_up_austria_ship_to_number.withColumn('Open_Qty_m3', summed_up_austria_ship_to_number.FullCalc_Phil-summed_up_austria_ship_to_number.sum_lifting_in_cbm) \
                      .withColumn("Erfüllung", when(summed_up_austria_ship_to_number.sum_FullCalc_Phil > 0.9,(summed_up_austria_ship_to_number.sum_sum_lifting_in_cbm/summed_up_austria_ship_to_number.sum_FullCalc_Phil*lit(100))).otherwise('').cast("double")) \
                    .withColumn("Produkt", when(upper(summed_up_austria_ship_to_number.Grade) == 'AGO','Diesel').when(upper(summed_up_austria_ship_to_number.Grade) == 'IGO','Heizöl').when(upper(summed_up_austria_ship_to_number.Grade) == 'MOGAS','Benzin').when(upper(summed_up_austria_ship_to_number.Grade).like('%GTL%'),summed_up_austria_ship_to_number.Subgrade).when(upper(summed_up_austria_ship_to_number.Grade) == 'FSD','Fuel Save Diesel').when(upper(summed_up_austria_ship_to_number.Grade) == 'LNG','LNG').otherwise('Other')) \
                    .withColumn("Abholzeitraum", when(summed_up_austria_ship_to_number.Valid_From.substr(6, 2) == month(current_date()),summed_up_austria_ship_to_number.Contract_Start_Period).when(summed_up_austria_ship_to_number.Valid_To.substr(6, 2) == month(current_date()),summed_up_austria_ship_to_number.Contract_End_Period).otherwise(summed_up_austria_ship_to_number.Contract_Start_Period)) \
                    .withColumn("Fullfillment_vs_Plan", when(summed_up_austria_ship_to_number.sum_lifting_in_cbm == 0,0).when(summed_up_austria_ship_to_number.Avg_Plan_m3 < 5,'').when(length(summed_up_austria_ship_to_number.FullCalc_Phil) == 0,'').otherwise(summed_up_austria_ship_to_number.sum_lifting_in_cbm/summed_up_austria_ship_to_number.FullCalc_Phil).cast("double")) \
                     .withColumn("Fullfillment_vs_Target", summed_up_austria_ship_to_number.sum_lifting_in_cbm/summed_up_austria_ship_to_number.Avg_Target_Quantity) \
                     .withColumn("Laufzeit", when(length(summed_up_austria_ship_to_number.Valid_From.substr(7, 2)) == 0,'').otherwise(datediff(summed_up_austria_ship_to_number.Valid_To,current_date())).cast("integer")) \
                     .withColumn("Avg_Plan_m3", ceil(summed_up_austria_ship_to_number.Avg_Plan_m3)) \
                     .withColumn("sum_lifting_in_to", round(summed_up_austria_ship_to_number.sum_lifting_in_to,2)) \
                     .withColumn('FullCalc_Phil', floor(summed_up_austria_ship_to_number.FullCalc_Phil)) \
                     .withColumn('Tolerance_m15', ceil(summed_up_austria_ship_to_number.Tolerance_m15)) \
                     .withColumn('Active', when(current_date().between(summed_up_austria_ship_to_number.Valid_From,summed_up_austria_ship_to_number.Valid_To),"Active").otherwise("Inactive"))

# COMMAND ----------

summed_up_Abholz_Trigger = summed_up_calculated_Fields.withColumn("Abholzeitraum", when(summed_up_calculated_Fields.Purchase_Order_Type.isin('TTTM','TTTS','TTTT','TTTI'),summed_up_calculated_Fields.Contract_Start_Period).otherwise(summed_up_calculated_Fields.Abholzeitraum)).drop("subgrade").drop('Volume_CBM')

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date,expr, row_number, add_months, concat_ws, current_timestamp
deals_wo_contract_num = df_mot.filter(df_mot.Contract_No.isNull()).drop_duplicates().withColumn("write_timestamp", current_timestamp())

# COMMAND ----------

import pyspark.sql.functions as F
summed_up_final = summed_up_Abholz_Trigger.withColumn("write_timestamp", F.from_utc_timestamp(current_timestamp(), 'GMT+1'))

# COMMAND ----------

#summed_up_final.filter(summed_up_final.Contract_No == '322446905').display()

# COMMAND ----------

df_mot.unpersist()

# COMMAND ----------

#summed = readLouvreDbTable(spark,"louvre_curated.Summed_up_on_Contract_Item")

# COMMAND ----------

#summed.count()

# COMMAND ----------

#summed_f = summed.dropDuplicates()

# COMMAND ----------

#writeLouvreDbTable(summed_f,"louvre_curated.Summed_up_on_Contract_Item")
