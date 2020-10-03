from sys import argv
from pyspark import SparkContext
import json
from itertools import combinations
from _datetime import datetime
start_time = datetime.now()
input_file = argv[1]
output_file = argv[2]
sc = SparkContext(appName='inf553')
lines = sc.textFile(input_file).map(lambda x: json.loads(x))
busi_list = lines.map(lambda x: x['business_id']).distinct().collect()
users_list = lines.map(lambda x: x['user_id']).distinct().collect()
users = dict([(u, ind) for ind, u in enumerate(users_list)])
user_len = len(users)
threshold = 0.05


def minhash_sig(user_ids):
    signatures = []
    for a in range(1, 350, 7):
        for b in range(1, 20, 7):
            hashed_ids = []
            for user_id in user_ids:
                r = ((a * user_id + b) % 149399) % user_len
                hashed_ids.append(r)
            signatures.append(min(hashed_ids))
    return signatures


def lsh(signatures, r):
    buckets = []
    for band_no, band in enumerate(range(0, len(signatures), r)):
        bucket = hash(tuple(signatures[band: band + r]))
        buckets.append((band_no, bucket))
    return buckets


def jaccard_sim(a,b):
    return len(set(a).intersection(set(b))) / len(set(a).union(set(b)))


b_u_matrix = lines.map(lambda x: (x['business_id'], x['user_id']))\
    .groupByKey().map(lambda x: (x[0], [users[user_id] for user_id in list(set(x[1]))]))
min_sig_matrix = b_u_matrix.map(lambda x: (x[0], minhash_sig(x[1])))
lsh_pairs = min_sig_matrix.map(lambda x: (x[0], lsh(x[1], 1)))\
    .flatMap(lambda x: [(bucket, x[0]) for bucket in x[1]])\
    .groupByKey().map(lambda x:  set(x[1]))\
    .filter(lambda x: len(x) > 1)\
    .flatMap(lambda x: [tuple(sorted(pair)) for pair in list(combinations(x, 2))])\
    .distinct()
b_u = dict(b_u_matrix.collect())
verified_pair = lsh_pairs.map(lambda x: (x, jaccard_sim(b_u[x[0]], b_u[x[1]])))\
    .filter(lambda x: x[1] >= threshold)

with open(output_file, 'w') as f:
    for pair in verified_pair.collect():
        f.write("{\"b1\": " + "\"" + pair[0][0] + "\", ")
        f.write("\"b2\": " + "\"" + pair[0][1] + "\", ")
        f.write("\"sim\": " + str(pair[1]) + "}")
        f.write('\n')

end_time = datetime.now()
duration = end_time - start_time
print("Duration:", duration)

