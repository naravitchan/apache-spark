


import pyspark


# spark-submit --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" delta.py

# from delta.tables import *
spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# pyspark --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
from delta.tables import *
# data = spark.range(0, 5)
# data.write.format("delta").save("/opt/bitnami/spark/datasets/delta")

# data = spark.range(5, 10)
# data.write.format("delta").mode("overwrite").save("/opt/bitnami/spark/datasets/delta")

df = spark.read.format("delta").load("file:///opt/bitnami/spark/datasets/delta")
df.show()

spark.stop()
