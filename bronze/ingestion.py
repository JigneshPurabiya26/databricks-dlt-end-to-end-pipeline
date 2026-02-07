import dlt


#Ingesting Sales Data
@dlt.table(
    name = 'sales_bronze'
)
def sales_bronze():
    df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'csv')\
        .load('/Volumes/databricks_catalog_jignesh/bronze/bronze_volume/sales/')
    return df


#Ingesting Customers Data
@dlt.table(
    name = 'customers_bronze'
)
def customers_bronze():
    df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'csv')\
        .load('/Volumes/databricks_catalog_jignesh/bronze/bronze_volume/customers/')
    return df


#Ingesting Products Data
@dlt.table(
    name = 'products_bronze'
)
def products_bronze():
    df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'csv')\
        .load('/Volumes/databricks_catalog_jignesh/bronze/bronze_volume/products/')
    return df


#Ingesting Stores Data
@dlt.table(
    name = 'stores_bronze'
)
def stores_bronze():
    df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'csv')\
        .load('/Volumes/databricks_catalog_jignesh/bronze/bronze_volume/stores/')
    return df
