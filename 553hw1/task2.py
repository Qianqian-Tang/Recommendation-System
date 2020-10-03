from pyspark import SparkContext
import json
from sys import argv
review_file = argv[1]
business_file = argv[2]
output_file = argv[3]
if_spark = argv[4]
n = argv[5]
if if_spark == "spark":
    # spark version
    sc = SparkContext(appName="inf553")
    rdd_review = sc.textFile(review_file)\
        .map(lambda x: json.loads(x)).\
        map(lambda x: (x['business_id'], x['stars']))
    rdd_business = sc.textFile(business_file)\
        .map(lambda x: json.loads(x)).\
        map(lambda x: (x['business_id'], x['categories']))
    category_star_rdd = rdd_review.join(rdd_business).map(lambda x: x[1]).flatMapValues(lambda x: str(x).split(",")).map(
        lambda x: (str(x[1]).strip(), x[0]))
    sum_cnt = category_star_rdd.aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1),
                                               lambda x, y: (x[0] + y[0], x[1] + y[1]))
    cate_avg = sum_cnt.map(lambda x: (x[0], x[1][0] / x[1][1]))
    cate_avg = cate_avg.sortBy(lambda x: (-x[1], x[0]), True).take(20)
    dict = {"result": cate_avg}
else:
    # nospark version
    review_data = []
    file = open(review_file, "r")
    for line in file.readlines():
        review_dic = json.loads(line)
        review_data.append(review_dic)
    business_data = []
    file = open(business_file, "r")
    for line in file.readlines():
        business_dic = json.loads(line)
        business_data.append(business_dic)
    id_cate = {}
    for line in business_data:
        key1 = line['business_id']
        id_cate[key1] = str(line['categories']).split(",")
    cate_star = {}
    for line in review_data:
        if line['business_id'] in id_cate.keys():
            for cate in id_cate[line['business_id']]:
                if cate.strip() in cate_star.keys():
                    cate_star[cate.strip()].append(line['stars'])
                else:
                    cate_star[cate.strip()] = [line['stars']]
    cate_ave_star = []
    for key, values in cate_star.items():
        cate_ave_star.append([key, sum(values)/len(values)])
    desc_cate_star = sorted(cate_ave_star, key = lambda item: ((-1*item[1],item[0])), reverse = False)
    dict = {"result": desc_cate_star[:int(n)]}
with open(output_file, 'w') as json_file:
    json.dump(dict, json_file)