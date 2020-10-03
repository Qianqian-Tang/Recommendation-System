import os
import pyspark
from sys import argv
from pyspark import SparkContext
from itertools import combinations
from pyspark.sql import SQLContext
from graphframes import *
import datetime
start_time = datetime.datetime.now()
filter_threshold = int(argv[1])
input_file = argv[2]
output_file = argv[3]
os.environ["PYSPARK_SUBMIT_ARGS"] = (
"--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
scConf = pyspark.SparkConf()\
    .setAppName('hw4')\
    .setMaster('local[3]')
sc = SparkContext.getOrCreate(conf=scConf)
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)
lines = sc.textFile(input_file)
header = lines.first()
lines = lines.map(lambda x: x.split(','))\
    .filter(lambda x: x[0] != header)\
    .map(lambda x: (x[1], x[0]))\
    .groupByKey().map(lambda x: (x[0], list(x[1])))\
    .map(lambda x: [(tuple(sorted(u_u)), 1) for u_u in list(combinations(x[1], 2))])\
    .flatMap(lambda x: x)\
    .reduceByKey(lambda x, y: x+y)\
    .filter(lambda x: x[1] >= filter_threshold)\
    .map(lambda x: x[0])
users = lines.flatMap(lambda x: x).distinct().map(lambda x: (x,))
edges1 = lines.map(lambda x: (x[1], x[0])).collect()
edges2 = lines.collect()
edges1.extend(edges2)
vertices = sqlContext.createDataFrame(users.collect(), ["id"])
edges = sqlContext.createDataFrame(edges1, ["src", "dst"])
g = GraphFrame(vertices, edges)
result = g.labelPropagation(maxIter=5)\
    .rdd.map(lambda x: (x[1], x[0]))\
    .groupByKey().map(lambda x: (x[0], list(x[1])))\
    .map(lambda x: (len(x[1]), sorted(x[1])))\
    .groupByKey().map(lambda x: (x[0], list(x[1])))\
    .mapValues(lambda x: sorted(x, key=lambda y: y[0]))\
    .sortByKey().values().collect()
with open(output_file, 'w') as f:
    for same_len_commu in result:
        for community in same_len_commu:
            f.write(str(community).strip('[]'))
            f.write('\n')
end_time = datetime.datetime.now()
print("duration: ", end_time - start_time)
