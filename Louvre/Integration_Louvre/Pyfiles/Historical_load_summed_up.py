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

# Load the current data and the history data
current_data = readLouvreDbTable(spark,'louvre_curated.Summed_up_on_Contract_Item')
history_data = readLouvreDbTable(spark,'louvre_curated.summed_up_historical')

# COMMAND ----------

current_data.count()

# COMMAND ----------

history_data.count()

# COMMAND ----------

common_cols = list(set(current_data.columns) & set(history_data.columns))
common_cols.remove('Key')

# COMMAND ----------


df_merged = history_data.alias('a').join( \
    current_data.alias('b'), ['Key'], how='outer' \
).select('Key', *(sf.coalesce('b.' + col, 'a.' + col).alias(col) for col in common_cols))

# COMMAND ----------

df_merged.count()

# COMMAND ----------

#append data to historical table
appendLouvreDbTable(df_merged,'louvre_curated.summed_up_historical')
