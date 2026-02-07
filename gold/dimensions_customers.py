import dlt
from pyspark.sql.functions import *

#creating gold streaming view on top of silver streaming view
@dlt.view(
    name = 'customers_gold_view'
)
def customers_gold_view():
    df_customers = spark.readStream.table('customers_silver_view')
    return df_customers


#creating empty gold streaming table on 
dlt.create_streaming_table(
    name = 'dim_customers'
)


#create gold streaming table on top of gold view with scd - 2
dlt.create_auto_cdc_flow(
    target='dim_customers',
    source='customers_gold_view',
    keys = ['customer_id'],
    sequence_by= col('processed_date'),
    stored_as_scd_type= 2,
    except_column_list= ['processed_date']
)