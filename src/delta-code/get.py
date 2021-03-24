import pyspark

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from delta.tables import *
from pyspark.sql.functions import *

df = spark.read.format("delta").load("file:///opt/bitnami/spark/datasets/delta")
df.orderBy("id").show(df.count())

# # VERSION
# df = spark.read.format("delta").option("versionAsOf", 0).load("file:///opt/bitnami/spark/datasets/delta")
# df.show()
# # GMT TIME
# df1 = spark.read.format("delta").option("timestampAsOf", "2021-03-23T04:57:00.000Z").load("file:///opt/bitnami/spark/datasets/delta")
# df.show()