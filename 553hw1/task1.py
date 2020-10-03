from pyspark import SparkContext, SparkConf
import json
from sys import argv
import re
input_file = argv[1]
output_file = argv[2]
stopwords = argv[3]
y = argv[4]
m = argv[5]
n = argv[6]

# confSparK = SparkConf().setAppName('inf553').setMaster('local[*]').set('spark.driver.memory', '12G')
# sc = SparkContext.getOrCreate(confSparK)
sc = SparkContext(appName = 'inf553')
lines = sc.textFile(input_file)
lines = lines.map(lambda x: json.loads(x))
rdd_stopwords = sc.textFile(stopwords).map(lambda x: (x, 1))

# A. The total number of reviews
value_a = lines.count()

# B. The number of reviews in a given year, y
value_b = lines.filter(lambda x: x['date'].startswith(str(y))).count()

# C. The number of distinct users who have written the reviews
value_c = lines.map(lambda x: x['user_id']).distinct().count()

# D. Top m users who have the largest number of reviews and its count
value_d = lines.map(lambda x: (x['user_id'],1)).reduceByKey(lambda x, x2: x+x2).sortBy(lambda x: ((-1)*x[1],x[0]), True).take(int(m))

# E. Top n frequent words in the review text. The words should be in lower cases.
# The following punctuations i.e., “(”, “[”, “,”, “.”, “!”, “?”, “:”, “;”, “]”, “)”, and the given stopwords are excluded
value_e = lines.map(lambda x: re.sub('[([,.!?:;\])]', ' ', x['text'].lower()))\
    .flatMap(lambda x: x.split())\
    .map(lambda x: (x, 1))\
    .subtractByKey(rdd_stopwords)\
    .reduceByKey(lambda x,y:x+y)\
    .sortBy(lambda x: (-x[1], x[0]), True)\
    .map(lambda x: x[0])\
    .take(int(n))

dict = {"A": value_a,"B": value_b,"C": value_c, "D": value_d, "E": value_e}
with open(output_file,'w') as json_file:
    json.dump(dict, json_file)
