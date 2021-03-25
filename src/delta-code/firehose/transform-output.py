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

def mapper(line):
    print(line)
    row=json.loads(line)
    account_id=int(row['account_id'])
    serial_id=str(row['serial_id'])
    data_dt=datetime.strptime(row['data_dt'], '%Y-%m-%d %H:%M:%S')
    cr_dt=datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S')
    up_dt=datetime.strptime(row['updated_at'], '%Y-%m-%d %H:%M:%S')
    payload=row['payload']
    # print(serial_id)

    results = list()

    # payload will be array[dict] only one dict
    for key in payload:
        # print(key)
        each_metric = Row(
            account_id=account_id,
            serial_id=serial_id,
            data_dt=data_dt,
            measure_name=str(key),
            measure_value_varchar=str(payload[key]),
            created_at=cr_dt,
            updated_at=up_dt
        )
        results.append(each_metric)
    return results


lines = spark.sparkContext.textFile(path)
people = lines.flatMap(mapper)
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.show(10)
schemaPeople.printSchema()
schemaPeople.write.format("delta").save("file:///opt/bitnami/spark/datasets/thing_outputs_stream-delta")



# # # auto structure
# extract = spark\
#     .read\
#     .json(path)

# # extract.show(extract.count(), False)
# extract.printSchema()

# extract.write.format("json").save("file:///opt/bitnami/spark/datasets/thing_payload_stream_extract.json")

# measure_name measure_value::