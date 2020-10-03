from sys import argv
from pyspark import SparkContext
import json
from _datetime import datetime
start_time = datetime.now()
train_file = argv[1]
test_file = argv[2]
model_file = argv[3]
output_file = argv[4]
cf_type = argv[5]
sc = SparkContext(appName='inf553')
train_lines = sc.textFile(train_file).map(lambda x: json.loads(x))
model_lines = sc.textFile(model_file).map(lambda x: json.loads(x))
test_lines = sc.textFile(test_file).map(lambda x: json.loads(x))

if cf_type == 'item_based':
    def unique_b_star(u_stars):
        uni_dict = {}
        uniq = []
        for b_st in u_stars:
            if b_st[0] not in uni_dict.keys():
                uni_dict[b_st[0]] = b_st[1]
            else:
                uni_dict[b_st[0]] = (uni_dict[b_st[0]] + b_st[1]) / 2
        for b, star in uni_dict.items():
            uniq.append((b, star))
        return uniq

    def predict_star(bb_s_list):
        sim_star = []
        for bb_s in bb_s_list:
            if bb_s[0] in b1_b2_sim_model_dict.keys():
                sim_star.append((b1_b2_sim_model_dict[bb_s[0]], bb_s[1]))
        most_similar = sorted(sim_star, key=lambda x: x[0], reverse=True)
        most_similar = most_similar[:5]
        numera = []
        denomi = []
        for w_s in most_similar:
            numera.append(w_s[0] * w_s[1])
            denomi.append(abs(w_s[0]))
        numerator = sum(numera)
        denominator = sum(denomi)
        if denominator == 0:
            pred = None
        else:
            pred = numerator / denominator
        return pred
    b1_b2_sim_model = model_lines.map(lambda x: ((x['b1'], x['b2']), x['sim'])).collect()
    b1_b2_sim_model_dict = dict(b1_b2_sim_model)
    u_b_test = test_lines.map(lambda x: (x['user_id'], x['business_id']))
    u_b_s_train = train_lines.map(lambda x: (x['user_id'], x['business_id'], x['stars']))\
        .map(lambda x: (x[0], (x[1], x[2])))\
        .groupByKey().map(lambda x: (x[0], list(x[1])))\
        .map(lambda x: (x[0], unique_b_star(x[1])))
    prediction = u_b_test.join(u_b_s_train)\
        .map(lambda x: ((x[0], x[1][0]), [(tuple(sorted([x[1][0], b_s[0]])), b_s[1]) for b_s in x[1][1]]))\
        .map(lambda x: (x[0], predict_star(x[1])))\
        .filter(lambda x: x[1] != None)
    with open(output_file, 'w') as f:
        for pair in prediction.collect():
            f.write("{\"user_id\": " + "\"" + pair[0][0] + "\", ")
            f.write("\"business_id\": " + "\"" + pair[0][1] + "\", ")
            f.write("\"stars\": " + str(pair[1]) + "}")
            f.write('\n')
else:
    def unique_u_star(u_stars):
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


    def predict_star(uu_s_list):
        sim_star_avgu2 = []
        avg_u2 = u_avg_s_dict[uu_s_list[0][0][0]]
        for uu_s in uu_s_list:
            if uu_s[0] in u1_u2_sim_model_dict.keys():
                sim_star_avgu2.append((u1_u2_sim_model_dict[uu_s[0]], uu_s[1], u_avg_s_dict[uu_s[0][1]]))
        numera = []
        denomi = []

        for w_s_avg2 in sim_star_avgu2:
            numera.append(w_s_avg2[0] * (w_s_avg2[1]-w_s_avg2[2]))
            denomi.append(abs(w_s_avg2[0]))
        numerator = sum(numera)
        denominator = sum(denomi)
        if denominator == 0:
            pred = None
        else:
            pred = numerator / denominator + avg_u2
        return pred

    u_avg_s = train_lines.map(lambda x: (x['user_id'], x['stars']))\
        .groupByKey().map(lambda x: (x[0], list(x[1])))\
        .map(lambda x: (x[0], sum(x[1]) / len(x[1])))

    u_avg_s_dict = dict(u_avg_s.collect())

    u1_u2_sim_model = model_lines.map(lambda x: ((x['u1'], x['u2']), x['sim'])).collect()
    u1_u2_sim_model_dict = dict(u1_u2_sim_model)
    b_u_test = test_lines.map(lambda x: (x['business_id'], x['user_id']))
    b_u_s_train = train_lines.map(lambda x: (x['business_id'], x['user_id'], x['stars'])) \
        .map(lambda x: (x[0], (x[1], x[2]))) \
        .groupByKey().map(lambda x: (x[0], list(x[1]))) \
        .map(lambda x: (x[0], unique_u_star(x[1])))
    prediction = b_u_test.join(b_u_s_train) \
        .map(lambda x: ((x[0], x[1][0]), [(tuple(sorted([x[1][0], u_s[0]])), u_s[1]) for u_s in x[1][1]])) \
        .map(lambda x: (x[0], predict_star(x[1]))) \
        .filter(lambda x: x[1] != None)
    with open(output_file, 'w') as f:
        for pair in prediction.collect():
            f.write("{\"user_id\": " + "\"" + pair[0][0] + "\", ")
            f.write("\"business_id\": " + "\"" + pair[0][1] + "\", ")
            f.write("\"stars\": " + str(pair[1]) + "}")
            f.write('\n')

end_time = datetime.now()
duration = end_time - start_time
print("Duration:", duration)


