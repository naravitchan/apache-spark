from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

import collections

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))


lines = spark.sparkContext.textFile(
    "file:///opt/bitnami/spark/datasets/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# # # SQL can be run over DataFrames that have been registered as a table.
# teenagers = spark.sql("SELECT age, avg(numFriends) FROM people GROUP BY age")

# # The results of SQL queries are RDDs and support all the normal RDD operations.
# for teen in teenagers.collect():
#     print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").avg('numFriends').show()

schemaPeople.groupBy("age").avg('numFriends').sort("age").show()

schemaPeople.groupBy("age").agg(func.round(
    func.avg("numFriends"), 2)).sort("age").show()

schemaPeople.groupBy("age").agg(func.round(
    func.avg("numFriends"), 2).alias("numFriends_avg")).sort("age").show()

spark.stop()
# Have to stop
