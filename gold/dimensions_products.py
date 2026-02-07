import dlt
from pyspark.sql.functions import *

#creating gold streaming view on top of silver streaming view
@dlt.view(
    name = 'products_gold_view'
)
def products_gold_view():
    df_products = spark.readStream.table('products_silver_view')
    return df_products


#creating empty gold streaming table on 
dlt.create_streaming_table(
    name = 'dim_products'
)


#create gold streaming table on top of gold view with scd - 2
dlt.create_auto_cdc_flow(
    target='dim_products',
    source='products_gold_view',
    keys = ['product_id'],
    sequence_by= col('processed_date'),
    stored_as_scd_type= 2,
    except_column_list= ['processed_date']
)