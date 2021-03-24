import pyspark

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, "file:///opt/bitnami/spark/datasets/delta")
deltaTable.toDF().orderBy("id").show()

# Update every even value by adding 100 to it
deltaTable.update(
  condition = expr("id%2==0"),
  set = { "id": expr("id+100")})

deltaTable.update(
  condition = expr("id%2==0"),
  set = { "id": expr("id+100")})

deltaTable.update(
  condition = expr("id%2==0"),
  set = { "id": expr("id+100")})
# # Delete every even value
# deltaTable.delete(condition = expr("id%2==0"))

# # Upsert (merge) new data
# newData = spark.range(0, 20)

# deltaTable.alias("oldData") \
#   .merge(
#     newData.alias("newData"),
#     "oldData.id=newData.id") \
#   .whenMatchedUpdate(set = { "id": col("newData.id") }) \
#   .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
#   .execute()

deltaTable.toDF().orderBy("id").show()