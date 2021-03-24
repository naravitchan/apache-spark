import pyspark
from pyspark.sql import Row
from pyspark.sql.functions import regexp_extract

# spark-submit --packages io.delta:delta-core_2.12:0.8.0

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from delta.tables import *

# path="file:///opt/bitnami/spark/datasets/delta-2/increment"
# checkpointPath="file:///opt/bitnami/spark/datasets/delta-2/checkpoint"
# # numFiles=4
# accessLines = spark.readStream.text("file:///opt/bitnami/spark/datasets/logs")

# # accessLines.writeStream.trigger
# contentSizeExp = r'\s(\d+)$'
# statusExp = r'\s(\d{3})\s'
# generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
# timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
# hostExp = r'(^\S+\.[\S+\.]+\S+)\s'
# logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
#                             regexp_extract('value', timeExp,
#                                            1).alias('timestamp'),
#                             regexp_extract('value', generalExp,
#                                            1).alias('method'),
#                             regexp_extract('value', generalExp,
#                                            2).alias('endpoint'),
#                             regexp_extract('value', generalExp,
#                                            3).alias('protocol'),
#                             regexp_extract('value', statusExp, 1).cast(
#                                 'integer').alias('status'),
#                             regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# # logsDF.show()
# query = logsDF.writeStream.trigger(once=True).format('delta') \
#     .option('checkpointLocation', checkpointPath) \
#     .start(path)

# # Run forever until terminated
# query.awaitTermination()

# spark.read.format("delta").load(path).show()

# Cleanly shut down the session
spark.stop()

