import pyspark
from pyspark.sql import Row

# spark-submit --packages io.delta:delta-core_2.12:0.8.0

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from delta.tables import *

path="file:///opt/bitnami/spark/datasets/delta-1"
numFiles=4

# # #1
# def mapper(line):
#     fields = line.split(',')
#     return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))


# lines = spark.sparkContext.textFile(
#     "file:///opt/bitnami/spark/datasets/fakefriends.csv")
# people = lines.map(mapper)

# # Infer the schema, and register the DataFrame as a table.
# schemaPeople = spark.createDataFrame(people)
# schemaPeople.show()

# schemaPeople.write.format("delta").partitionBy("age").save(path)

# # #2
# df = spark.read.format("delta").option("versionAsOf", 0).load(path)
# df.orderBy('age').show(df.count())

# # #3
spark.read.format("delta").load(path).repartition(numFiles)\
    .write.option("dataChange", "false") \
    .format("delta").mode("overwrite").save(path)
