from sys import argv
from pyspark import SparkContext
import json
import math
from _datetime import datetime
start_time = datetime.now()
test_file = argv[1]
model_file = argv[2]
output_file = argv[3]
sc = SparkContext(appName='inf553')
test_lines = sc.textFile(test_file).map(lambda x: json.loads(x))
test_u = test_lines.map(lambda x: x['user_id']).distinct().collect()
test_b = test_lines.map(lambda x: x['business_id']).distinct().collect()
model_lines = sc.textFile(model_file).map(lambda x: json.loads(x))
business_profile = model_lines.map(lambda x: (x['id'], x['type'], x['vector']))\
    .filter(lambda x: x[1] == 'business')\
    .filter(lambda x: x[0] in test_b)\
    .map(lambda x: (x[0], x[2]))
business_pf = dict(business_profile.collect())
user_profile = model_lines.map(lambda x: (x['id'], x['type'], x['vector']))\
    .filter(lambda x: x[1] == 'user')\
    .filter(lambda x: x[0] in test_u)\
    .map(lambda x: (x[0], x[2]))
user_pf = dict(user_profile.collect())
user_busi = test_lines.map(lambda x: (x['user_id'], x['business_id']))\
    .filter(lambda x: x[0] in user_pf.keys())\
    .filter(lambda x: x[1] in business_pf.keys())\
    .map(lambda x: (x[0], x[1], len(set(user_pf[x[0]]).intersection(set(business_pf[x[1]]))) / (math.sqrt(len(user_pf[x[0]])) * math.sqrt(len(business_pf[x[1]])))))\
    .filter(lambda x: x[2] >= 0.01)
with open(output_file, 'w') as f:
    for pair in user_busi.collect():
        f.write("{\"user_id\": " + "\"" + pair[0] + "\", ")
        f.write("\"business_id\": " + "\"" + pair[1] + "\", ")
        f.write("\"sim\": " + str(pair[2]) + "}")
        f.write('\n')

end_time = datetime.now()
duration = end_time - start_time
print("Duration:", duration)