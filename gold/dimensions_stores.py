import dlt
from pyspark.sql.functions import *

#creating gold streaming view on top of silver streaming view
@dlt.view(
    name = 'stores_gold_view'
)
def stores_gold_view():
    df_stores = spark.readStream.table('stores_silver_view')
    return df_stores


#creating empty gold streaming table on 
dlt.create_streaming_table(
    name = 'dim_stores'
)


#create gold streaming table on top of gold view with scd - 2
dlt.create_auto_cdc_flow(
    target='dim_stores',
    source='stores_gold_view',
    keys = ['store_id'],
    sequence_by= col('processed_date'),
    stored_as_scd_type= 2,
    except_column_list= ['processed_date']
)