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
from pyspark.sql.functions import unix_timestamp,datediff, to_date, lit,current_date,expr, row_number, concat_ws, current_timestamp, month, round, ceil, year
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

CDB_Plan_Vol_Price_Period = readLouvreDbTable(spark,"louvre_curated.Contract_Data_2023")

# COMMAND ----------

select_data = CDB_Plan_Vol_Price_Period.filter((year(col("Contract_Start_Date")) == "2023") & (year(col("Contract_end_Date")) == "2023"))

# COMMAND ----------

#select_data = filter.withColumnRenamed("customer","Sold_To_Name")


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
transpose = select_data.selectExpr('incoterm','mot','supply_point','depot_property','density','pricing_period','agreemt_period', \
                    'FED_Contract','sold_to_number', \
                    'ship_to_number','Material_Number','price_basis',"stack(12, '202301',PlanCBM_Jan, '202302',PlanCBM_Feb, \
          '202303',PlanCBM_Mar,'202304',PlanCBM_April,'202305',PlanCBM_May,'202306',PlanCBM_Jun,'202307',PlanCBM_Jul,'202308',PlanCBM_Aug, \
          '202309',PlanCBM_Sep,'202310',PlanCBM_Oct,'202311',PlanCBM_Nov,'202312',PlanCBM_Dec)") \
		  .withColumnRenamed("col0","Term_Plan_Period") \
          .withColumnRenamed("col1","Plan_m3")

# COMMAND ----------

transpose.createOrReplaceTempView("transpose")

# COMMAND ----------

formula = spark.sql("""select 
incoterm,
mot,
supply_point,
depot_property,
density,
Plan_m3,
Plan_m3*Density as Plan_to,
pricing_period,
agreemt_period,
fed_contract,
sold_to_number,
ship_to_number,
Material_Number,
Price_Basis,
concat(Left(Term_Plan_Period, 4),'-',Substring(Term_Plan_Period,5,2)) as Term_Plan_Period from transpose""")
formula.createOrReplaceTempView("formula")

# COMMAND ----------

group_by = spark.sql("""select 
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
ship_to_number,
material_number,
price_basis,
Term_Plan_Period
from formula
group by
incoterm,
mot,
supply_point,
depot_property,
density,
pricing_period,
agreemt_period,
fed_contract,
sold_to_number,
ship_to_number,
material_number,
price_basis,
Term_Plan_Period""")
group_by.createOrReplaceTempView("group_by")

# COMMAND ----------

after_groupby = spark.sql("""select incoterm,
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
Price_Basis,
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

writeLouvreDbTable(after_groupby,"louvre_curated.Transformed_CDB_Data_2023")

# COMMAND ----------


