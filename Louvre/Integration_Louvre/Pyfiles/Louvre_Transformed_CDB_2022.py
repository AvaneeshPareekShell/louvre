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
from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date,expr, row_number, concat_ws, current_timestamp, month, round, ceil
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import count, avg,sum
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

CDB_Plan_Vol_Price_Period = readLouvreDbTable(spark,"louvre_curated.Contract_Data")

# COMMAND ----------

select_data = CDB_Plan_Vol_Price_Period.filter(col("contract_status") == "active")

# COMMAND ----------

from datetime import datetime
current_timestamp = datetime.now()
year = str(current_timestamp.year)
print(year)
jan = year+"01"
print(jan)

# COMMAND ----------

from datetime import datetime
current_timestamp = datetime.now()
year = current_timestamp.year
transpose = select_data.selectExpr('Sold_To_Name','incoterm','mot','supply_point','depot_property','density','pricing_period','agreemt_period', \
                    'fed_contract','sold_to_number', \
                    'price_basis','ship_to_number','Material_Number','tracking_status',"stack(12, '202201',planvol_jan, '202202',planvol_feb, \
          '202203',planvol_mar,'202204',planvol_apr,'202205',planvol_may,'202206',planvol_jun,'202207',planvol_jul,'202208',planvol_aug, \
          '202209',planvol_sep,'202210',planvol_oct,'202211',planvol_nov,'202212',planvol_dec)") \
		  .withColumnRenamed("col0","Term_Plan_Period") \
          .withColumnRenamed("col1","Plan_to")

# COMMAND ----------

transpose.createOrReplaceTempView("transpose")

# COMMAND ----------

formula = spark.sql("""select 
Sold_To_Name,
incoterm,
mot,
supply_point,
depot_property,
density,
Plan_to/Density as Plan_m3,
pricing_period,
agreemt_period,
fed_contract,
sold_to_number,
price_basis,
ship_to_number,
Material_Number,
tracking_status,
concat(Left(Term_Plan_Period, 4),'-',Substring(Term_Plan_Period,5,2)) as Term_Plan_Period,
Plan_to from transpose""")
formula.createOrReplaceTempView("formula")

# COMMAND ----------

group_by = spark.sql("""select 
Sold_To_Name,
incoterm,
mot,
supply_point,
depot_property,
density,
sum(Plan_m3) as Plan_m3,
sum(Plan_to) as Plan_to,
pricing_period,
agreemt_period,
fed_contract,
sold_to_number,
price_basis,
ship_to_number,
material_number,
Term_Plan_Period,
tracking_status
from formula
group by
Sold_To_Name,
incoterm,
mot,
supply_point,
depot_property,
density,
pricing_period,
agreemt_period,
fed_contract,
sold_to_number,
price_basis,
ship_to_number,
material_number,
Term_Plan_Period,
tracking_status""")
group_by.createOrReplaceTempView("group_by")

# COMMAND ----------

after_groupby = spark.sql("""select --Sold_To_Name,
incoterm,
mot,
case when supply_point = 'D333'
      then 'D361'
else supply_point end as supply_point,
Plan_m3,
depot_property,
density,
pricing_period,
agreemt_period,
fed_contract,
sold_to_number,
ship_to_number,
material_number,
Term_Plan_Period,
price_basis,
--tracking_status,
Plan_to,
case when mot = 'ITT' then "In-Tank"
     when mot = 'Barge' and Incoterm = 'FOB' then "Vessel Pickup"
	 when mot = 'Barge' and Incoterm != 'FOB' then "Vessel Delivery"
	 when mot = 'Rail' and Incoterm = 'FCA' then "Rail Pickup"
	 when mot = 'Rail' and Incoterm != 'FCA' then "Rail Delivery"
	 when mot != 'ITT' and MoT != 'Barge' and MoT != 'Rail' and Incoterm = 'FCA' then "Road Pickup"
else 'Road Delivery' end as Shipping_Condition_Term
from group_by
""")

# COMMAND ----------

Louvre_Transformed_CDB_2022_Row_num = after_groupby.withColumn("Row_number",row_number().over(Window.partitionBy("material_number","ship_to_number","supply_point",
"term_plan_period").orderBy(after_groupby.Plan_m3.desc())))
Louvre_Transformed_CDB_2022 = Louvre_Transformed_CDB_2022_Row_num.filter(Louvre_Transformed_CDB_2022_Row_num.Row_number == 1).drop(Louvre_Transformed_CDB_2022_Row_num.Row_number)

# COMMAND ----------

writeLouvreDbTable(Louvre_Transformed_CDB_2022,"louvre_curated.Transformed_CDB_Data_2022")
