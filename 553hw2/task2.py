from pyspark import SparkContext
from sys import argv
from itertools import chain
import re
import math
from _datetime import datetime
start_time = datetime.now()
filter_threshold = int(argv[1])
support = int(argv[2])
input_file = argv[3]
output_file = argv[4]
sc = SparkContext(appName='inf553')
# 1st phase of SON
candidates = dict()
def find_candidates(iterator):
    singletons = []
    baskets = []
    baskest_list = []
    freq_singles = dict()
    for basket in iterator:
        baskest_list.append(basket)
        baskets.extend(basket)
    # find frequent singletons
    for single in set(baskets):
        freq_singles[single] = 0
    for single in baskets:
        freq_singles[single] += 1
    for singleton, count in freq_singles.items():
        if count >= math.ceil(support * len(baskest_list) / total_baskets):
            singletons.append(singleton)
    i = 1
    candidates = dict()
    while True:
        candidates[i] = []
        tuples_count = dict()
        # from previous frequents, find tuples whose subsets with maximum size are frequent
        for basket in baskest_list:
            pre_tuples = []
            if i == 1:
                pre_freq = singletons
                for pre_tuple in pre_freq:
                    if pre_tuple in basket:
                        pre_tuples.append(pre_tuple)
                if pre_tuples != []:
                    sorted_item = sorted(pre_tuples)
                    for pretup in pre_tuples:
                        for item in sorted_item:
                            if pretup < item:
                                pretup2 = pretup
                                appendpretup = tuple(sorted([pretup2, item]))
                                if appendpretup in tuples_count.keys():
                                    tuples_count[appendpretup] += 1
                                else:
                                    tuples_count[appendpretup] = 1
                                if tuples_count[appendpretup] == math.ceil(support * len(baskest_list) / total_baskets):
                                    candidates[i].append(appendpretup)
            else:
                for pre_tuple in pre_freq:
                    if set(chain(pre_tuple)).issubset(basket):
                        pre_tuples.append(pre_tuple)
                if pre_tuples != []:
                    sorted_item = sorted(set(chain.from_iterable(pre_tuples)))
                    for pretup in pre_tuples:
                        for item in sorted_item:
                            if pretup[-1] < item:
                                pretup2 = list(pretup)
                                pretup2.append(item)
                                appendpretup = tuple(pretup2)
                                # print(appendpretup)
                                if appendpretup in tuples_count.keys():
                                    tuples_count[appendpretup] += 1
                                else:
                                    tuples_count[appendpretup] = 1
                                if tuples_count[appendpretup] == math.ceil(support * len(baskest_list) / total_baskets):
                                    candidates[i].append(appendpretup)
        candidates[i] = sorted(candidates[i])
        pre_freq = candidates[i]
        if candidates[i] == []:
            break
        # flat the frequent tuples to singles
        i += 1
    singletons2 = []
    for i in singletons:
        singletons2.append(tuple([i]))
    candidates[0] = singletons2
    candidates[0] = sorted(candidates[0])
    candidates.pop(len(candidates) - 1)
    return candidates.values()
def rdd_to_sorted_list(candidate_output):
    max_len = 0
    for candidate in candidate_output:
        if len(candidate) > max_len:
            max_len = len(candidate)
    ouput_dict = dict()
    for i in range(max_len):
        ouput_dict[i] = []
        for item in candidate_output:
            if len(item) == i + 1:
                ouput_dict[i].append(item)
        ouput_dict[i] = ouput_dict[i]
    result = []
    result.append(ouput_dict[0])
    ouput_dict[0] = re.sub(',\)', ')', str(ouput_dict[0]))
    for i in range(1, len(ouput_dict)):
        result.append(ouput_dict[i])
    return result, max_len
def count_freq(iterator):
    for basket in iterator:
        for candidate in list(chain.from_iterable(candidates_result)):
            if set(candidate).issubset(basket):
                yield candidate
# build user_business baskets model
user_business_rdd = sc.textFile(input_file)\
    .map(lambda line: line.split(','))\
    .filter(lambda line: line[0] != 'user_id')
user_business_rdd = user_business_rdd\
    .map(lambda u_b: (u_b[0], u_b[1]))\
    .distinct()
user_business_basket = user_business_rdd.groupByKey()\
    .map(lambda x: (x[0], list(x[1])))
candidate_output = user_business_basket\
    .filter(lambda x:len(x[1])>filter_threshold)\
    .map(lambda x: x[1])
total_baskets = candidate_output.count()
candidate_output = candidate_output\
    .mapPartitions(find_candidates)\
    .flatMap((lambda x: x))\
    .distinct()\
    .map(lambda x: (x,1))\
    .sortByKey()\
    .map(lambda x:x[0])\
    .collect()
candidates_result = rdd_to_sorted_list(candidate_output)[0]
max_len = rdd_to_sorted_list(candidate_output)[1]
# 2nd phase of SON
freq_itemsets = dict()
freq_output = user_business_basket\
    .filter(lambda x:len(x[1])>filter_threshold)\
    .map(lambda x: x[1])\
    .mapPartitions(count_freq).map(lambda x:(x,1))\
    .reduceByKey(lambda x1,x2:x1+x2)\
    .filter(lambda x: x[1]>=support)\
    .map(lambda x:(x[0],1))\
    .sortByKey()\
    .map(lambda x:x[0])\
    .collect()
freq_result = rdd_to_sorted_list(freq_output)[0]
max_len_freq = rdd_to_sorted_list(freq_output)[1]
with open(output_file, 'w') as f:
    f.write("Candidates:" + '\n')
    for i in range(max_len):
        a = re.sub(',\)', ')', str(candidates_result[i]).strip('[]'))
        a = re.sub('\), ', '),',a)
        f.write(a)
        f.write('\n\n')
    f.write("Frequent Itemsets:" + '\n')
    for i in range(max_len_freq):
        a = re.sub(',\)', ')', str(freq_result[i]).strip('[]'))
        a = re.sub('\), ', '),', a)
        f.write(a)
        f.write('\n\n')
end_time = datetime.now()
duration = end_time - start_time
print("Duration:", duration)