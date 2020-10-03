from pyspark import SparkContext
import json
from sys import argv

input_file = argv[1]
output_file = argv[2]
partition_type = argv[3]
n_partitions = int(argv[4])
n = argv[5]


sc = SparkContext(appName="inf553")

if(partition_type == "customized"):
    # customized partition
    rdd_review = sc.textFile(input_file).map(lambda x: json.loads(x)).\
        map(lambda line: (line['business_id'], 1)).\
        partitionBy(n_partitions, lambda key: hash(key[0]))
else:
    # default partition
    rdd_review = sc.textFile(input_file).map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], 1))

n_partitions_out = rdd_review.getNumPartitions()
n_items = rdd_review.glom().map(lambda x: len(x)).collect()

business_count = rdd_review.reduceByKey(lambda x1, x2: x1+x2).filter(lambda x: (x[1]> int(n))).map(lambda x: [x[0],x[1]]).collect()
dict = {"n_partitions": n_partitions_out, "n_items": n_items, "result": business_count}
with open(output_file, 'w') as json_file:
    json.dump(dict, json_file)