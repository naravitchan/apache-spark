from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read each row to dataframe
inputDF = spark.read.text("file:///opt/bitnami/spark/datasets/book.txt")
# explode same as flatmap
words = inputDF.select(func.explode(
    func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")
# words.show(words.count())

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())

spark.stop()
