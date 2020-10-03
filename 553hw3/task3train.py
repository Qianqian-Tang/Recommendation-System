from sys import argv
from pyspark import SparkContext
import json
import math
from itertools import combinations
from _datetime import datetime
start_time = datetime.now()
train_file = argv[1]
model_file = argv[2]
cf_type = argv[3]
sc = SparkContext(appName='inf553')
lines = sc.textFile(train_file).map(lambda x: json.loads(x))
if cf_type == 'item_based':
    users_list = lines.map(lambda x: x['user_id']).distinct().collect()
    users = dict([(u, ind) for ind, u in enumerate(users_list)])
    def unique_user_star(u_stars):
        uni_dict = {}
        uniq = []
        for u_st in u_stars:
            if u_st[0] not in uni_dict.keys():
                uni_dict[u_st[0]] = u_st[1]
            else:
                uni_dict[u_st[0]] = (uni_dict[u_st[0]] + u_st[1]) / 2
        for u, star in uni_dict.items():
            uniq.append((u, star))
        return uniq


    def pearson_correlation(i_stars, j_stars):
        i_avg = sum(i_stars.values()) / len(i_stars)
        j_avg = sum(j_stars.values()) / len(j_stars)
        numerator_list = []
        for i_star in i_stars.keys():
            for j_star in j_stars.keys():
                if i_star == j_star:
                    numerator_list.append((i_stars[i_star] - i_avg) * (j_stars[j_star] - j_avg))
        numerator = sum(numerator_list)
        denomi_i_list = []
        for star in i_stars.values():
            denomi_i_list.append(math.pow(star - i_avg, 2))
        denomi_i = math.sqrt(sum(denomi_i_list))
        denomi_j_list = []
        for star in j_stars.values():
            denomi_j_list.append(math.pow(star - j_avg, 2))
        denomi_j = math.sqrt(sum(denomi_j_list))
        if denomi_i == 0 or denomi_j == 0:
            w_ij = None
        else:
            denominator = denomi_i * denomi_j
            w_ij = numerator / denominator
        return w_ij


    def b_pair_weight(user_star_list):
        user_star_dict = dict(user_star_list)
        b_w_list = []
        for b, u_star in b_u_star_dict.items():
            co_user = set(u_star.keys()).intersection(set(user_star_dict.keys()))
            if len(co_user) >= 3:
                i_star = {}
                j_star = {}
                for user in co_user:
                    i_star[user] = u_star[user]
                    j_star[user] = user_star_dict[user]
                b_w_list.append((b, pearson_correlation(i_star, j_star)))
        return b_w_list

    b_u_star_rdd = lines.map(lambda x: (x['business_id'], x['user_id'], x['stars']))\
        .map(lambda x: (x[0], (users[x[1]], x[2])))\
        .groupByKey().map(lambda x: (x[0], list(x[1])))\
        .map(lambda x: (x[0], unique_user_star(x[1])))
    b_u_stars = b_u_star_rdd.collect()
    b_u_star_dict = dict(b_u_stars)
    for k, v in b_u_star_dict.items():
        b_u_star_dict[k] = dict(v)
    b_pair_pearson = b_u_star_rdd.map(lambda x: (x[0], b_pair_weight(x[1])))\
        .filter(lambda x: x[1] != [])\
        .flatMap(lambda x: [((x[0], b_w[0]), b_w[1]) for b_w in x[1]])\
        .filter(lambda x: x[0][0] != x[0][1] and x[1] is not None)\
        .filter(lambda x: x[1] > 0)\
        .map(lambda x: (tuple(sorted(x[0])), x[1])).distinct()

    with open(model_file, 'w') as f:
        for pair in b_pair_pearson.collect():
            f.write("{\"b1\": " + "\"" + pair[0][0] + "\", ")
            f.write("\"b2\": " + "\"" + pair[0][1] + "\", ")
            f.write("\"sim\": " + str(pair[1]) + "}")
            f.write('\n')
else:
    busi_list = lines.map(lambda x: x['business_id']).distinct().collect()
    busi = dict([(b, ind) for ind, b in enumerate(busi_list)])
    user_list = lines.map(lambda x: x['business_id']).distinct().collect()
    users = dict([(b, ind) for ind, b in enumerate(user_list)])
    b_len = len(busi)
    threshold = 0.01


    def minhash_sig(b_ids):
        signatures = []
        for a in range(1, 50, 7):
            for b in range(1, 20, 7):
                hashed_ids = []
                for b_id in b_ids:
                    r = ((a * b_id + b) % 149399) % b_len
                    hashed_ids.append(r)
                signatures.append(min(hashed_ids))
        return signatures


    def lsh(signatures, r):
        buckets = []
        for band_no, band in enumerate(range(0, len(signatures), r)):
            bucket = hash(tuple(signatures[band: band + r]))
            buckets.append((band_no, bucket))
        return buckets


    def jaccard_sim(a, b):
        return len(set(a).intersection(set(b))) / len(set(a).union(set(b)))


    u_b_matrix = lines.map(lambda x: (x['user_id'], x['business_id'])) \
        .groupByKey().map(lambda x: (x[0], [busi[b_id] for b_id in list(set(x[1]))]))
    min_sig_matrix = u_b_matrix.map(lambda x: (x[0], minhash_sig(x[1])))
    lsh_pairs = min_sig_matrix.map(lambda x: (x[0], lsh(x[1], 1))) \
        .flatMap(lambda x: [(bucket, x[0]) for bucket in x[1]]) \
        .groupByKey().map(lambda x: set(x[1])) \
        .filter(lambda x: len(x) > 1) \
        .flatMap(lambda x: [tuple(sorted(pair)) for pair in list(combinations(x, 2))]) \
        .distinct()
    u_b = dict(u_b_matrix.collect())

    verified_pair = lsh_pairs.map(lambda x: (x, jaccard_sim(u_b[x[0]], u_b[x[1]]))) \
        .filter(lambda x: x[1] >= threshold)

    filtered_u = verified_pair.map(lambda x: x[0]).flatMap(lambda x: x).distinct().map(lambda x: (x, None))
    def unique_b_star(b_stars):
        uni_dict = {}
        uniq = []
        for b_st in b_stars:
            if b_st[0] not in uni_dict.keys():
                uni_dict[b_st[0]] = b_st[1]
            else:
                uni_dict[b_st[0]] = (uni_dict[b_st[0]] + b_st[1]) / 2
        for b, star in uni_dict.items():
            uniq.append((b, star))
        return uniq


    def pearson_correlation(i_stars, j_stars):
        i_avg = sum(i_stars.values()) / len(i_stars)
        j_avg = sum(j_stars.values()) / len(j_stars)
        numerator_list = []
        for i_star in i_stars.keys():
            for j_star in j_stars.keys():
                if i_star == j_star:
                    numerator_list.append((i_stars[i_star] - i_avg) * (j_stars[j_star] - j_avg))
        numerator = sum(numerator_list)
        denomi_i_list = []
        for star in i_stars.values():
            denomi_i_list.append(math.pow(star - i_avg, 2))
        denomi_i = math.sqrt(sum(denomi_i_list))
        denomi_j_list = []
        for star in j_stars.values():
            denomi_j_list.append(math.pow(star - j_avg, 2))
        denomi_j = math.sqrt(sum(denomi_j_list))
        if denomi_i == 0 or denomi_j == 0:
            w_ij = None
        else:
            denominator = denomi_i * denomi_j
            w_ij = numerator / denominator
        return w_ij


    def u_pair_weight(b_star_list):
        b_star_dict = dict(b_star_list)
        u_w_list = []
        for u, b_star in u_b_star_dict.items():
            co_busi = set(b_star.keys()).intersection(set(b_star_dict.keys()))
            if len(co_busi) >= 3:
                i_star = {}
                j_star = {}
                for b in co_busi:
                    i_star[b] = b_star[b]
                    j_star[b] = b_star_dict[b]
                u_w_list.append((u, pearson_correlation(i_star, j_star)))
        return u_w_list


    u_b_star_rdd = lines.map(lambda x: (x['user_id'], x['business_id'], x['stars'])) \
        .map(lambda x: (x[0], (busi[x[1]], x[2]))) \
        .groupByKey().map(lambda x: (x[0], list(x[1]))) \
        .map(lambda x: (x[0], unique_b_star(x[1])))
    u_b_stars = u_b_star_rdd.collect()
    u_b_star_dict = dict(u_b_stars)
    for k, v in u_b_star_dict.items():
        u_b_star_dict[k] = dict(v)
    u_pair_pearson = filtered_u.join(u_b_star_rdd)\
        .map(lambda x: (x[0], x[1][1]))\
        .map(lambda x: (x[0], u_pair_weight(x[1]))) \
        .filter(lambda x: x[1] != []) \
        .flatMap(lambda x: [((x[0], b_w[0]), b_w[1]) for b_w in x[1]]) \
        .filter(lambda x: x[0][0] != x[0][1] and x[1] is not None) \
        .filter(lambda x: x[1] > 0) \
        .map(lambda x: (tuple(sorted(x[0])), x[1])).distinct()

    with open(model_file, 'w') as f:
        for pair in u_pair_pearson.collect():
            f.write("{\"u1\": " + "\"" + pair[0][0] + "\", ")
            f.write("\"u2\": " + "\"" + pair[0][1] + "\", ")
            f.write("\"sim\": " + str(pair[1]) + "}")
            f.write('\n')

end_time = datetime.now()
duration = end_time - start_time
print("Duration:", duration)


