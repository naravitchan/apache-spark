# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import codecs

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# names = spark.read.schema(schema).option("sep", " ").csv(
#     "file:///opt/bitnami/spark/datasets/Marvel-names.txt")


def loadMovieNames():
    name = {}
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)])

    names = spark.read.schema(schema).option("sep", " ").csv(
        "file:///opt/bitnami/spark/datasets/Marvel-names.txt")
    names.show()
    # print(names)
    for n in names.collect():
        if n['name']:
            na = n['name'].encode('utf-8')
        name[n['id']] = na
        # print(n['id'])
        # print(na)
    # print(names.show())
    # print(name)
    return name


# def loadNames():
#     name = {}
#     schema = StructType([
#         StructField("id", IntegerType(), True),
#         StructField("name", StringType(), True)])

#     names = spark.read.schema(schema).option("sep", " ").csv(
#         "file:///opt/bitnami/spark/datasets/Marvel-names.txt")
#     for n in names.collect():
#         name[n[0]] = n[1]
#     print(name)
#     return name


nameDict = spark.sparkContext.broadcast(loadMovieNames())


lines = spark.read.text("file:///opt/bitnami/spark/datasets/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("super_hero_id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("super_hero_id").agg(func.sum("connections").alias("connections"))

# Create a user-defined function to look up movie names from our broadcasted dictionary


def lookupName(superHeroId):
    # print(nameDict.value[int(superHeroId)])
    return nameDict.value[int(superHeroId)]
    # return nameDict.value[superHeroId]


lookupNameUDF = func.udf(lookupName)  # user-defined function

# connections.show()
# Add a movieTitle column using our new udf
connectionWithNames = connections.withColumn(
    "name", lookupNameUDF((func.col("super_hero_id"))))
connectionWithNames.show(10, False)

spark.stop()
