import dlt
from pyspark.sql.functions import *

#Streaming Stores View
@dlt.view(
    name = 'stores_silver_view'
)
def stores_silver_view():
    df_stores = spark.readStream.table('stores_bronze')
    df_stores = df_stores.withColumn('store_name',regexp_replace(col("store_name"),"_"," "))\
                    .withColumn('processed_date',current_timestamp())
    return df_stores



#Stores Silver Table with upsert
dlt.create_streaming_table(
    name = 'stores_silver'
)

dlt.create_auto_cdc_flow(
    target='stores_silver',
    source='stores_silver_view',
    keys = ['store_id'],
    sequence_by= col('processed_date'),
    stored_as_scd_type= 1
)