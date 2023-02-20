# Databricks notebook source
# MAGIC %run /Louvre/Generic_Functions/connectionConfig/ADLSConnection

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/properties

# COMMAND ----------

# MAGIC %run /Louvre/Generic_Functions/connectionConfig/SQLConnector

# COMMAND ----------

import sys
from pyspark.sql import  SparkSession, SQLContext
from time import gmtime, strftime, time
from pyspark.sql.functions import col,abs, to_timestamp, date_format,coalesce,regexp_replace,substring,trim,countDistinct
from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date, month
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import count, avg,sum
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_date
from pyspark.sql.functions import concat, concat_ws
from pyspark.sql.functions import when
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

spark.conf.set("spark.sql.shuffle.partitions",'30')

# COMMAND ----------

# MAGIC %run /Louvre/Integration_Louvre/Pyfiles/AllMapped_Raw

# COMMAND ----------

#fetching TC NextGen data
df_TC_NextGen = AllMapped_Raw.filter(col('MATNR').isNotNull() & col('zxbldat').isNotNull()).dropDuplicates()

# COMMAND ----------

#renaming columns from TC NextGen join
df_TC_NextGen_renamed = df_TC_NextGen.withColumnRenamed('INCO1','Incoterm_1') \
.withColumnRenamed('INCO2','Incoterm_Name') \
.withColumnRenamed('kunnr','Sold_To_Number')  \
.withColumnRenamed('OID_SHIP','Ship_To_Number') \
.withColumnRenamed('guebg','Valid_From') \
.withColumnRenamed('gueen','Valid_To') \
.withColumnRenamed('erdat','Contract_Creation_Period') \
.withColumnRenamed('erzet','Contract_Created_Time') \
.withColumnRenamed('ctlpc','Risk_Category') \
.withColumnRenamed('auart','Sales_Doc_Type') \
.withColumnRenamed('vsbed','Shipping_Condition') \
.withColumnRenamed('OID_MISCDL','Driver_Text') \
.withColumnRenamed('knumv','Document_Position_Number') \
.withColumnRenamed('ktext','Contract_Description') \
.withColumnRenamed('ZMENG','Target_Quantity') \
.withColumnRenamed('posnr','Item_Number') \
.withColumnRenamed('vbap_werks','Plant_Code') \
.withColumnRenamed('kunn2','National_Account_Number') \
.withColumnRenamed('BSARK_E','Purchase_Order_Type') \
.withColumnRenamed('PROD_GROUP_DESC','Product_Group_Description') \
.withColumnRenamed('OIC_PTRIP','License_Plate') \
.withColumnRenamed('Fkart','Invoice_Type') \
.withColumnRenamed('ZTERM','Payment_Term') \
.withColumnRenamed('ZLSCH','Payment_Method') \
.withColumnRenamed('OIHANTYP','Handling_Type') \
.withColumnRenamed('mblnr','Material_Document_Number') \
.withColumnRenamed('matnr','Material_Number') \
.withColumnRenamed('bukrs','Company_Code') \
.withColumnRenamed('ZXBLDAT','Loading_Date') \
.withColumnRenamed('BWART','Movement_Type') \
.withColumnRenamed('aedat','Last_Modified_Date') \
.withColumnRenamed('vtweg','Distribution_Channel') \
.withColumnRenamed('ZXOIB_BLTIME','Loading_Time')\
.withColumnRenamed('abgru','Reason_for_Rejection') \
.withColumnRenamed('vbeln','Contract_No') \
.withColumnRenamed('ZIEME','UoM')\
.withColumnRenamed('MSEG_SO','Sales_Order')\
.withColumnRenamed('OIVBELN','Delivery_Number')

# COMMAND ----------

#formatting columns by removing leading zeros
df_tc_nextgen_formatted = df_TC_NextGen_renamed.withColumn('Material_Number', sf.regexp_replace('Material_Number', r'^[0]*', '')) \
                          .withColumn('Ship_To_Number', sf.regexp_replace('Ship_To_Number', r'^[0]*', '')) \
                          .withColumn('Sold_To_Number', sf.regexp_replace('Sold_To_Number', r'^[0]*', '')) \
                          .withColumn('Sales_Order', sf.regexp_replace('Sales_Order', r'^[0]*', '')) \
                          .withColumn('Item_Number', sf.regexp_replace('Item_Number', r'^[0]*', '')) \
                          .withColumn('Contract_No', sf.regexp_replace('Contract_No', r'^[0]*', '')) \
                          .withColumn('Document_Position_Number', sf.regexp_replace('Document_Position_Number', r'^[0]*', '')) \
                          .withColumn('Item_Number', sf.regexp_replace('Item_Number', r'^[0]*', '')) 

# COMMAND ----------

#adding calculated columns 
df_TC_NextGen_calculated = df_tc_nextgen_formatted.withColumn('PoT_manual_update',when(df_tc_nextgen_formatted.Purchase_Order_Type.isNull(), 'Yes').otherwise('No')) \
                          .withColumn('Lifting_in_to', df_tc_nextgen_formatted.lifting_in_KG/1000) \
                          .withColumn('Lifting_in_cbm',df_tc_nextgen_formatted.lifting_in_L15/1000) \
                          .withColumn('Loading_Time',concat_ws('',col('Loading_Time').substr(1,2),lit(':'),col('Loading_Time').substr(3,2),lit(':'),col('Loading_Time').substr(5,2))) \
                          .withColumn('Contract_Creation_Period',concat_ws('',df_tc_nextgen_formatted.Contract_Creation_Period.substr(1, 4), lit('-'), df_tc_nextgen_formatted.Contract_Creation_Period.substr(5, 2))) \
                          .withColumn('Lifting_Period',concat_ws('',df_tc_nextgen_formatted.Loading_Date.substr(1, 4), lit('-'), df_tc_nextgen_formatted.Loading_Date.substr(5, 2))) \
                          .withColumn('Loading_Date',to_date(concat_ws('',df_tc_nextgen_formatted.Loading_Date.substr(1, 4), lit('-'), df_tc_nextgen_formatted.Loading_Date.substr(5, 2), lit('-'), df_tc_nextgen_formatted.Loading_Date.substr(7, 2)),'yyyy-MM-dd')) \
.withColumn('Valid_To',to_date(concat_ws('',df_tc_nextgen_formatted.Valid_To.substr(1, 4), lit('-'), df_tc_nextgen_formatted.Valid_To.substr(5, 2), lit('-'), df_tc_nextgen_formatted.Valid_To.substr(7, 2)),'yyyy-MM-dd')) \
.withColumn('Valid_From',to_date(concat_ws('',df_tc_nextgen_formatted.Valid_From.substr(1, 4), lit('-'), df_tc_nextgen_formatted.Valid_From.substr(5, 2), lit('-'), df_tc_nextgen_formatted.Valid_From.substr(7, 2)),'yyyy-MM-dd')) \
.withColumn('Billing_Date',to_date(concat_ws('',df_tc_nextgen_formatted.Billing_Date.substr(1, 4), lit('-'), df_tc_nextgen_formatted.Billing_Date.substr(5, 2), lit('-'), df_tc_nextgen_formatted.Billing_Date.substr(7, 2)),'yyyy-MM-dd'))

# COMMAND ----------

#changing date format for loading date
df_date_formatted = df_TC_NextGen_calculated.withColumn('Loading_Date_formatted',date_format(col('Loading_Date'), "dd.MM.yyyy"))

# COMMAND ----------

CDB_Plan_Vol_Price_Period_2022 = readLouvreDbTable(spark,"louvre_curated.Transformed_CDB_Data_2022")

# COMMAND ----------

CDB_Plan_Vol_Price_Period_2023 = readLouvreDbTable(spark,"louvre_curated.Transformed_CDB_Data_2023")

# COMMAND ----------

CDB_Plan_Vol_Price_Period = CDB_Plan_Vol_Price_Period_2022.unionAll(CDB_Plan_Vol_Price_Period_2023)

# COMMAND ----------

#getting data from contract database
df_cdb = CDB_Plan_Vol_Price_Period.select('incoterm','mot','supply_point','Plan_m3','depot_property','density','pricing_period','agreemt_period','fed_contract','ship_to_number','material_number','Term_Plan_Period','Plan_to','Shipping_Condition_Term','price_basis').dropDuplicates()

# COMMAND ----------

#joining contract database with TC and NextGen data
df_cdb_merged = df_date_formatted.join(df_cdb , (df_date_formatted.Ship_To_Number == df_cdb.ship_to_number) &
                                  (df_date_formatted.Material_Number == df_cdb.material_number) & (df_date_formatted.Plant_Code == df_cdb.supply_point) & (df_date_formatted.Contract_Start_Period == df_cdb.Term_Plan_Period),'full').drop(df_cdb.material_number).drop(df_cdb.ship_to_number).dropDuplicates()

# COMMAND ----------

#merging incoterm values into one single column
df_cdb_merged_calculated = df_cdb_merged.withColumn('Incoterm_1', when(df_cdb_merged.Incoterm_1.isNull(), df_cdb_merged.incoterm).otherwise(df_cdb_merged.Incoterm_1)) \
                                        .drop('incoterm').withColumnRenamed('Incoterm_1','Incoterm')

# COMMAND ----------

#getting data from salesforce tables
Mount_Path = LouvreConfig["dataSets"]["Louvre_ADLS"]["Mount_Path"]
SF_UnHarm_Path =  Mount_Path+LouvreConfig["dataSets"]["l_ADLS_Loc"]["RSO_Loc"]["unharm"]+"SALESFORCE_SALES_ORDER_PRICE_AUDIT_REPORT"+'/'
df_price_audit_report = ReadFile_UnHarm(spark,SF_UnHarm_Path).dropDuplicates(['SAP_Contract_Number','SHT_Number','Material_No','Plant_No','Volume_CBM'])

# COMMAND ----------

#formatting salesforce data
df_price_audit_formatted = df_price_audit_report.withColumn('SAP_Contract_Number', sf.regexp_replace('SAP_Contract_Number', r'^[0]*', '')) \
                                                .withColumn('Material_No', sf.regexp_replace('Material_No', r'^[0]*', '')) \
                                                .withColumnRenamed('CreatedDate', 'Created_Date_Time') \
                                                .withColumnRenamed('ship_to_number', 'ship_to_number_sf')                                                      .dropDuplicates()

# COMMAND ----------

#joining price audit report from salesforce with cdb merged data
df_salesforce_merged = df_cdb_merged_calculated.join(df_price_audit_formatted ,(df_cdb_merged_calculated.Contract_No == df_price_audit_formatted.SAP_Contract_Number) & (df_cdb_merged_calculated.Material_Number == df_price_audit_formatted.Material_No) & (df_cdb_merged_calculated.Plant_Code == df_price_audit_formatted.Plant_No) , "left").drop(df_price_audit_formatted.Material_No).drop(df_price_audit_formatted.Plant_No).drop(df_price_audit_formatted.SAP_Contract_Number) 

# COMMAND ----------

#dropping columns with no contract number
df_salesforce_unique = df_salesforce_merged.filter(col('Contract_No').isNotNull()).dropDuplicates()

# COMMAND ----------

#fetching and formatting oiklidr table data
df_oiklidr = readTcDbTable(spark,"RAW_TERMINAL_COCKPIT.oiklidr").select('lidno','lid3cod1','lidaddon','ca_timestamp')

df_ordered = df_oiklidr.withColumn("rn", sf.row_number() \
    .over(Window.partitionBy("lidaddon")
    .orderBy(sf.col("ca_timestamp").desc())))

df_latest = df_ordered.filter(sf.col("rn") == 1).drop("rn").drop('ca_timestamp')
df_oiklidr_renamed = df_latest.withColumn('lidno', sf.regexp_replace('lidno', r'^[0]*', '')) \
                               .withColumnRenamed('lidno', 'load_id') \
                               .withColumnRenamed('lid3cod1', '3rd_party_code') \
                               .withColumn('contractno',substring('lidaddon',1,10)) \
                               .withColumn('item_num',substring('lidaddon',11,6)) \
                               .withColumn('contractno', sf.regexp_replace('contractno', r'^[0]*', '')) \
                               .withColumn('item_num', sf.regexp_replace('item_num', r'^[0]*', '')).drop('lidaddon')

# COMMAND ----------

#fetching load ids from oiklidr table
df_oiklidr_merged = df_salesforce_unique.join(df_oiklidr_renamed, (df_salesforce_unique.Contract_No == df_oiklidr_renamed.contractno) & (df_salesforce_unique.Item_Number == df_oiklidr_renamed.item_num),'left')

# COMMAND ----------

#SI mapping
si_kam_mapping = readLouvreDbTable(spark,"louvre_raw.SI_KAM_Mapping").select("Customer","Partner_Sales_Group","Sales group SP","Partner").withColumnRenamed("Sales group SP","Sales_group_SP").withColumn("Partner",sf.regexp_replace('Partner', r'^[0]*', '')).withColumn("Customer",sf.regexp_replace('Customer', r'^[0]*', '')).dropDuplicates()
df_SI_mapped = df_oiklidr_merged.join(si_kam_mapping,df_oiklidr_merged["ship_to_number"] == si_kam_mapping["Partner"],"left").withColumnRenamed("Partner_Sales_Group","SI").drop("Customer").drop("Partner").drop("Sales_group_SP").dropDuplicates(['Contract_No','Material_Document_Number','Material_Number','Delivery_Number','Loading_Date'])

# COMMAND ----------

df_SI_mapped=df_SI_mapped.cache()

# COMMAND ----------

#fetching static tables from sql db
df_shipping_condition = readLouvreDbTable(spark,'louvre_raw.shipping_condition')
df_excise_duty = readLouvreDbTable(spark,'louvre_raw.excise_duty')
df_invoice_type_mapping = readLouvreDbTable(spark,'louvre_raw.invoice_type_mapping')
df_material_mapping = readLouvreDbTable(spark,'louvre_raw.material_mapping').select(col('Material Number').alias('material_no'),col('Mat deutsch').alias('mat_deutsch'),col('Base Grade').alias('base_grade'),col('Produkt ').alias('Produkt'))

# COMMAND ----------

#creating invoice_no_delivery_no table
df_delivery = df_SI_mapped.select('Invoice_Type','Billing_Date','Delivery_Number','SI',col('Invoice_Number').alias('Billing_Document')) \
                          .withColumn('Delivery_Number',sf.regexp_replace('Delivery_Number', r'^[0]*', '')) \
                          .dropDuplicates(['Invoice_Type','Billing_Date','Delivery_Number','SI'])
df_invoive_no_del_no = df_delivery.join(df_invoice_type_mapping, col('Invoice_Type') == col('GSAP Code'),"inner") \
                                  .withColumn('write_timestamp',sf.current_timestamp()) \
                                  .drop('GSAP Code').drop('Invoice Number').drop('SI').drop('Invoice_Type').dropDuplicates()

# COMMAND ----------

#writing invoice_no_delivery_number table to database
writeLouvreDbTable(df_invoive_no_del_no,"louvre_curated.Invoice_Numbers_Delivery_Numbers")

# COMMAND ----------

#fetching data from static tables
df_mat_mapping_merged = df_SI_mapped.join(df_material_mapping, df_oiklidr_merged.Material_Number == df_material_mapping.material_no,"left").drop(df_material_mapping.material_no)
df_excise_duty_merged = df_mat_mapping_merged.join(df_excise_duty, df_mat_mapping_merged.Handling_Type == col('Handling_Type'),"left").drop(col('Handling_Type'))
df_shipping_merged = df_excise_duty_merged.join(df_shipping_condition,df_excise_duty_merged.Shipping_Condition == df_shipping_condition.shipping_condition,"left").drop(df_shipping_condition.shipping_condition)

# COMMAND ----------

#cleaning calculated columns
import pyspark.sql.functions as F
from pyspark.sql.functions import col,abs, to_timestamp, date_format,coalesce,regexp_replace,substring,trim,countDistinct
from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date,current_timestamp
df_cleaned = df_SI_mapped.withColumn('key',concat_ws('',col('Contract_No'),col('Plant_Code'), col('Material_Number'), col('Term_Plan_Period') )) \
                            .withColumn('write_timestamp',F.from_utc_timestamp(current_timestamp(), 'GMT+1')) \
                            .drop('kunnr').drop('VBRP_Sales_Contract_Number').drop('VBAP_bukrs').drop('ebeln').drop('VBRP_Position_Number').drop('vbrk_knumv').drop('vkorg') \
                            .withColumn('Delivery_Number',sf.regexp_replace('Delivery_Number', r'^[0]*', '')) \
                            .filter(col('Contract_No').isNotNull())
df_final = df_cleaned.join(df_material_mapping, df_cleaned.Material_Number == df_material_mapping.material_no,"left") \
                        .drop(df_material_mapping.material_no).drop(df_material_mapping.mat_deutsch).drop(df_material_mapping.Produkt)

# COMMAND ----------

#mapping MoT corresponding to shipping conditions
barge = ['30','31','40','41']
rail = ['20','21']
truck = ['10','11']
itt = ['80']
df_mot = df_final.withColumn('mot', when(col('Shipping_Condition').isin(barge),'Barge').when(col('Shipping_Condition').isin(rail),'Rail').when(col('Shipping_Condition').isin(truck),'Truck').when(col('Shipping_Condition').isin(itt),'ITT').otherwise('')).dropDuplicates()

# COMMAND ----------

alteryx = readLouvreDbTable(spark,'louvre_raw.Altreyx_AllMapped').select('load_id_a','driver_text_a','license_plate_a','material_document_number_a','Contract_Number','Material_Number_a')

# COMMAND ----------

lou_alt_join = df_mot.join(alteryx, (df_mot.Material_Document_Number == alteryx.material_document_number_a) & (df_mot.Material_Number == alteryx.Material_Number_a) & (df_mot.Contract_No == alteryx.Contract_Number),"left") \
                    .drop('material_document_number_a').drop('Material_Number_a').drop('Contract_Number').drop('lidaddon').drop('docno')

# COMMAND ----------

#backloading load_id
missing_field_added_fields = lou_alt_join.withColumn('load_id', when(col('load_id').isNull(),col('load_id_a')).otherwise(col('load_id'))) \
                                  .withColumn('license_plate', when(col('license_plate').isNull(),col('license_plate_a')).otherwise(col('license_plate'))) \
                                  .withColumn('driver_text', when(col('driver_text').isNull(),col('driver_text_a')).otherwise(col('driver_text'))) \
                                  .withColumn('license_plate', sf.regexp_replace('license_plate', '�', '')) \
                                  .drop('load_id_a').drop('driver_text_a').drop('license_plate_a') \
                                  .withColumn("Abholzeitraum", when(lou_alt_join.Valid_From.substr(6, 2) == month(current_date()),lou_alt_join.Contract_Start_Period).when(lou_alt_join.Valid_To.substr(6, 2) == month(current_date()),lou_alt_join.Contract_End_Period).otherwise(lou_alt_join.Contract_Start_Period))

# COMMAND ----------

#add missing fields
missing_field_added = missing_field_added_fields.withColumn("Abholzeitraum", when(missing_field_added_fields.Purchase_Order_Type.isin('TTTM','TTTS','TTTT','TTTI'),missing_field_added_fields.Contract_Start_Period).otherwise(missing_field_added_fields.Abholzeitraum))

# COMMAND ----------

#adding id for each record
#df_id = missing_field_added.withColumn('Id', concat_ws('',col('Contract_No'),col('Material_Document_Number'),col('Material_Number')))

# COMMAND ----------

#remove unwanted material no
removed_material_no = missing_field_added.filter(col('Material_Number')!='400011650').drop('VBRP_posnr','MSEG_MATNR','OIPOSNR','MSEG_WERKS','Nextgen_read_timestamp','kschl','VBFA_posnv','Material_DN').dropDuplicates().drop("Volume_Unit","PO_Type","ship_to_number_sf","Sales_Organization","Price_Condition")

# COMMAND ----------

#writing final table to sql database
writeLouvreDbTable(removed_material_no,'louvre_curated.all_mapped_excl_billing_doc')

# COMMAND ----------

df_SI_mapped.unpersist()
