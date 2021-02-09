from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)


sc = spark.sparkContext()

rdd = sc.textFile("C:\\test.txt")
print(rdd.collect())