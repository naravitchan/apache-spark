from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    customerId = fields[0]
    cost = float(fields[2])
    return (customerId, cost)


lines = sc.textFile("file:///opt/bitnami/spark/datasets/customer-orders.csv")
parsedLines = lines.map(parseLine)

customer = parsedLines.reduceByKey(
    lambda x, y: x + y)  # count

# wordCounts.map()
cost = customer.map(lambda x: (x[1], x[0])).sortByKey()
# cost = customer.map(lambda x,y: (y,x)).sortByKey() can be this
results = cost.collect()

for cost, customerId in results:
    print(cost + ":\t\t" + customerId)
