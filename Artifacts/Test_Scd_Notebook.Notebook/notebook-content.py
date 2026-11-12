# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d1eb9a9-9406-40b3-a024-c3d0b4fd4029",
# META       "default_lakehouse_name": "Scd_Type2_Test2_LH",
# META       "default_lakehouse_workspace_id": "022b37d7-57ad-42cb-993a-9ba2a3691076",
# META       "known_lakehouses": [
# META         {
# META           "id": "5d1eb9a9-9406-40b3-a024-c3d0b4fd4029"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Scd_Type2_Test2_LH.dbo.data_scd ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE if  NOT EXISTS control.water(
    table_name string,
    last_update TIMESTAMP
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
MERGE into control.water as t
USING (select 'cust' as table_name, '1900-01-01 00:00:00' as last_update) as s
on t.table_name = s.table_name
when not MATCHED THEN
INSERT (table_name, last_update) values(s.table_name, s.last_update)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w_date = spark.sql("""
select last_update from control.water
""")
l_date = w_date.collect()[0][0]

display(l_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

incremental_df = df.filter(col('last_modified')>lit(l_date))
display(incremental_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

incremental_df = incremental_df.withColumn('effective_start_date', col('last_modified'))\
                .withColumn('effective_end_date', to_date(lit(None)))\
                .withColumn('is_flag',lit(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

incremental_df.write.format('delta').mode('overwrite').save("Tables/sil/dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

target_df = spark.sql("""
select * from sil.dim_customer where is_flag=1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(target_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

incremental_df.createOrReplaceTempView('stg_customer')
target_df.createOrReplaceTempView('dim_customer_current')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW stg_source AS
# MAGIC SELECT
# MAGIC Null as mergekey,
# MAGIC s.CustomerID,
# MAGIC s.CustomerName,
# MAGIC s.City,
# MAGIC s.Country ,
# MAGIC s.effective_start_date,
# MAGIC s.effective_end_date,
# MAGIC s.is_flag,
# MAGIC s.last_modified
# MAGIC from stg_customer s  inner join dim_customer_current t
# MAGIC on s.CustomerID = t.CustomerID
# MAGIC where s.City<>t.City
# MAGIC OR s.Country <> t.Country
# MAGIC OR s.CustomerName<> t.CustomerName
# MAGIC 
# MAGIC union all 
# MAGIC 
# MAGIC select 
# MAGIC CustomerID as mergekey,
# MAGIC CustomerID,
# MAGIC CustomerName,
# MAGIC City,
# MAGIC Country,
# MAGIC effective_start_date,
# MAGIC effective_end_date,
# MAGIC is_flag,
# MAGIC last_modified
# MAGIC from stg_customer
# MAGIC 
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC Merge into sil.dim_customer as t
# MAGIC using stg_source as s
# MAGIC on t.CustomerID=s.mergekey
# MAGIC and t.is_flag= 1
# MAGIC when matched 
# MAGIC and t.CustomerName<>s.CustomerName
# MAGIC or t.City<>s.City
# MAGIC Or t.Country<>t.Country
# MAGIC Then update set
# MAGIC t.effective_end_date = CURRENT_TIMESTAMP(),
# MAGIC t.is_flag=0
# MAGIC when not matched then
# MAGIC insert(CustomerID, CustomerName, City, Country, effective_start_date, effective_end_date, is_flag, last_modified)
# MAGIC values(s.CustomerID, s.CustomerName, s.City, S.Country, s.effective_start_date, s.effective_end_date, s.is_flag, s.last_modified)
# MAGIC 
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tg = spark.sql("""
select * from sil.dim_customer
""")
display(tg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

d_date = incremental_df.agg(max(col('last_modified'))).collect()[0][0]
display(d_date)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if d_date is None:
    print("No Incremental Date Found")
else:
    print("New Incremental Date Found")

    spark.sql(f"""
        UPDATE control.water
        SET last_update = TIMESTAMP('{d_date}')
    """)
    




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql   
# MAGIC select last_update from control.water


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
