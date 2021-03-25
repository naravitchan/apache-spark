import pyspark
from datetime import datetime
import json
from pyspark.sql import Row,SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

path = '/opt/bitnami/spark/datasets/thing_outputs_stream-extract.txt'
# path = "/opt/bitnami/spark/datasets/thing_payload_stream-extract.txt"
# # # # RDD
def mapper(line):
    # try?
    a=json.loads(line)
    print(a)
    data_dt=datetime.strptime(a['data_dt'], '%Y-%m-%d %H:%M:%S')
    cr_dt=datetime.strptime(a['created_at'], '%Y-%m-%d %H:%M:%S')
    up_dt=datetime.strptime(a['updated_at'], '%Y-%m-%d %H:%M:%S')
    # return Row(ID=int(a['account_id']), payload=str(json.dumps(a['payload'])), token=str(a['token']), created_at=dt)
    return Row(
        account_id=int(a['account_id']),
        serial_id=int(a['serial_id']),
        data_dt=data_dt,
        payload=str(json.dumps(a['payload'])),
        created_at=cr_dt,
        updated_at=up_dt
    )


lines = spark.sparkContext.textFile(path)
people = lines.map(mapper)
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.show(10)
schemaPeople.printSchema()


# # # auto structure
# extract = spark\
#     .read\
#     .json(path)

# # extract.show(extract.count(), False)
# extract.printSchema()

# extract.write.format("json").save("file:///opt/bitnami/spark/datasets/thing_payload_stream_extract.json")

# measure_name measure_value::