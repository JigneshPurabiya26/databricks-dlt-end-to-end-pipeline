import dlt
from pyspark.sql.functions import *

#Streaming Sales View
@dlt.view(
    name = 'sales_silver_view'
)
def sales_silver_view():
    df_sales = spark.readStream.table('sales_bronze')
    df_sales = df_sales.withColumn('price_per_sale',round(col('total_amount')/col('quantity'),2))\
                    .withColumn('processed_date',current_timestamp())
    return df_sales



#Sales Silver Table with upsert
dlt.create_streaming_table(
    name = 'sales_silver'
)

dlt.create_auto_cdc_flow(
    target='sales_silver',
    source='sales_silver_view',
    keys = ['sales_id'],
    sequence_by= col('processed_date'),
    stored_as_scd_type= 1
)