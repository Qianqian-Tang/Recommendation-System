from pyspark import SparkContext
import json
import csv
from sys import argv
business_file = argv[1]
user_file = argv[2]
sc = SparkContext(appName='inf553')
business = sc.textFile(business_file)\
    .map(lambda x: json.loads(x))\
    .map(lambda x: (x['business_id'], x['state']))\
    .filter(lambda x: x[1] == 'NV')
user = sc.textFile(user_file)\
    .map(lambda x: json.loads(x))\
    .map(lambda x: (x['business_id'], x['user_id']))

user_busi = business.join(user)\
    .map(lambda x: [x[1][1], x[0]]).collect()
with open('user_business.csv', 'w') as f:
    writer = csv.DictWriter(f,fieldnames = ['user_id', 'business_id'])
    writer.writeheader()
    writer = csv.writer(f)
    writer.writerows(user_busi)

# command
# spark-submit preprocess.py business.json review.json