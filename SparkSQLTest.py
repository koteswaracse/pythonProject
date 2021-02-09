from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Spark Example').master("local[*]").getOrCreate()

people_schema = StructType([StructField("name", StringType(), True), StructField("age", StringType(), True),
                            StructField("dept_id", StringType(), True)])
people_data = spark.sparkContext.textFile("C:\\data\\people.txt").map(lambda x: x.split(','))
people = spark.createDataFrame(people_data, people_schema)
people = people.select(people.name, people.age.cast("int").alias('age'), people.dept_id.cast("int").alias('dept_id'))
print(people.count())
people.dropDuplicates()
print(people.count())

department_schema = StructType(
    [StructField("dept_id", StringType(), True), StructField("dept_name", StringType(), True)])
department_data = spark.sparkContext.textFile("C:\\data\\department.txt").map(lambda x: x.split(','))
department = spark.createDataFrame(department_data, department_schema)
department = department.select(department.dept_id.cast("int").alias('dept_id'), department.dept_name)
print(department.count())
department.dropDuplicates()
print(department.count())

department_people = people.where('dept_id in (10,20,30)').join(department, people.dept_id == department.dept_id) \
    .groupBy(people.name, people.age, department.dept_name).agg(F.max(department.dept_id).alias('dept_id'))
department_peopleSorted = department_people.orderBy(department_people.name, department_people.dept_id.desc())
department_peopleSorted.dropDuplicates()

#print(department_peopleSorted.collect())

# department_peopleSorted.coalesce(1).write.mode("overwrite").save("C:\\data\\out_put")
#department_peopleSorted.coalesce(1).rdd.saveAsTextFile("file:///C:/data/out_put/")

department_peopleSorted.registerTempTable("test_table")
people.registerTempTable("people_table")

query = '''
select people.name,people.age,people_dept.dept_id from people_table people 
join test_table people_dept on people.dept_id = people_dept.dept_id
where people.age < 33
'''

print("Final Result Dataset")
print(spark.sql(query).show())