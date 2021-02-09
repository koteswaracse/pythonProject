from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as psf

spark = SparkSession.builder.appName("SparkJob").master("local[*]").getOrCreate()
sc = spark.sparkContext

landingSchema = StructType([StructField('SALE_ID', StringType(), True),\
                            StructField('PRODUCT_ID', StringType(), True),\
                            StructField('QUANTITY_SOLD', IntegerType(), True),\
                            StructField('VENDOR_ID', StringType(), True),\
                            StructField('SALE_DATE', TimestampType(), True),\
                            StructField('SALE_AMOUNT', DoubleType(), True),\
                            StructField('SALE_CURRENCY', StringType(), True)])

landingFileDF = spark.read.schema(landingSchema).option("delimiter", ",").csv("file:///C:\data\sales")
print(landingFileDF.collect())

validData = landingFileDF.filter(psf.col("QUANTITY_SOLD").isNotNull())
print(validData.collect())


inValidData = landingFileDF.filter(psf.col("VENDOR_ID").isNull() | psf.col("QUANTITY_SOLD").isNull())\
    .withColumn("Hold Reason", psf.when(psf.col("QUANTITY_SOLD").isNull(),"Quantity Sold is Missing")\
                .otherwise(psf.when(psf.col("VENDOR_ID").isNull(), "Vendor Id is Missing")))
print(inValidData.collect())

'''
validData.write.mode("overwrite").option("delimiter", "|").csv("file:///C:\data\sales\output\valid")
inValidData.write.mode("overwrite").option("delimiter", "|").csv("file:///C:\data\sales\output\inValid")
'''