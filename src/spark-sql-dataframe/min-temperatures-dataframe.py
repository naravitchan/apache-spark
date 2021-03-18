from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("stationID", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv(
    "file:///opt/bitnami/spark/datasets/1800.csv")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

# Convert temperature to fahrenheit and sort the dataset
# withColumn - create new column name temperature
minTempsByStationF = minTempsByStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
    .select("stationID", "temperature").sort("temperature")

# Collect, format, and print the results
results = minTempsByStationF.show()

# for result in results:
#     print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
