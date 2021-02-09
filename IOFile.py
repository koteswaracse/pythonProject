import os
from pyspark.sql import SparkSession
from os import path
spark = SparkSession.builder.appName('Spark Example').master("local[*]").getOrCreate()

tempdir = "C:\\data\\"

'''
path = os.path.join(tempdir, "sample-text.txt")

with open(path, "w") as testFile:
    _ = testFile.write("Helloo world!")
textFile = spark.sparkContext.textFile(path)
print(textFile.collect())

parallelized = spark.sparkContext.parallelize(["World!"])
print(sorted(spark.sparkContext.union([textFile, parallelized]).collect()))

'''
dirPath = os.path.join(tempdir, "files")
textFiles = spark.sparkContext.wholeTextFiles(dirPath)
print(textFiles.collect())
