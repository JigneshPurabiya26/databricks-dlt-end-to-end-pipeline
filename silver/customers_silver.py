import dlt
from pyspark.sql.functions import *

#Streaming customers View
@dlt.view(
    name = 'customers_silver_view'
)
def customers_silver_view():
    df_customers = spark.readStream.table('customers_bronze')
    df_customers = df_customers\
                    .withColumn('name',upper(col('name')))\
                    .withColumn('domains',split(col('email'),"@")[1])\
                    .withColumn('processed_date',current_timestamp())
    return df_customers



#customers Silver Table with upsert
dlt.create_streaming_table(
    name = 'customers_silver'
)

dlt.create_auto_cdc_flow(
    target='customers_silver',
    source='customers_silver_view',
    keys = ['customer_id'],
    sequence_by= col('processed_date'),
    stored_as_scd_type= 1
)