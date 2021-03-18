from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("cost", FloatType(), True)])

df = spark.read.schema(schema).csv(
    "file:///opt/bitnami/spark/datasets/customer-orders.csv")
df.printSchema()

customers = df.select("customer_id", "cost")

customerCosts = customers.groupBy("customer_id").agg(func.round(
    func.sum("cost"), 2).alias("sumCost"))
customerCosts.sort(customerCosts.sumCost.desc()).show(customerCosts.count())

# example of sort
# >>> df.sort(df.age.desc()).collect()
# [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
# >>> df.sort("age", ascending=False).collect()
# [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
# >>> df.orderBy(df.age.desc()).collect()
# [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
# >>> from pyspark.sql.functions import *
# >>> df.sort(asc("age")).collect()
# [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
# >>> df.orderBy(desc("age"), "name").collect()
# [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
# >>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
# [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]

spark.stop()
