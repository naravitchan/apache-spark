import pyspark
from datetime import datetime
import json
from pyspark.sql import Row,SparkSession

# spark-submit --packages io.delta:delta-core_2.12:0.8.0

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
path = "/opt/bitnami/spark/datasets/thing_outputs_stream-extract.txt"
df = spark.read.format("delta").option("versionAsOf", 0).load("file:///opt/bitnami/spark/datasets/thing_outputs_stream-delta")
df.show()

# create view
df.createOrReplaceTempView("thing_output_1")
outputs = spark.sql("SELECT * FROM thing_output_1 limit 100")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for o in outputs.collect():
    print(o)

# We can also use functions instead of SQL queries:
# df.groupBy("age").count().orderBy("age").show()

spark.stop()