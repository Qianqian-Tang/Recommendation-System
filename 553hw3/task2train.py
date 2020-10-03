from sys import argv
from pyspark import SparkContext
import json
import re
import math
from _datetime import datetime
start_time = datetime.now()
input_file = argv[1]
model_file = argv[2]
stopwords = argv[3]
sc = SparkContext(appName='inf553')
lines = sc.textFile(input_file).map(lambda x: json.loads(x))
stopwords = sc.textFile(stopwords).map(lambda x: (x, 1))


def tf_idf(words):
    word_dict = {}
    for w in words:
        if w in word_dict.keys():
            word_dict[w] += 1
        else:
            word_dict[w] = 1
    max_freq = max(word_dict.values())
    for w in words:
        word_dict[w] = (word_dict[w] / max_freq) * math.log((N / n_dict[w]), 2)
    a = sorted(word_dict.items(), key=lambda x: x[1], reverse=True)
    return a[:200]


b_text = lines.map(lambda x: (x['business_id'], x['text']))\
    .groupByKey().map(lambda x: (x[0], list(x[1])))\
    .map(lambda x: (x[0], str(x[1]).replace('!\'', '')))\
    .map(lambda x: (x[0], x[1].replace('.\'', ''))) \
    .map(lambda x: (x[0], x[1].replace(', \'', ''))) \
    .map(lambda x: (x[0], x[1].replace('\\n',''))) \
    .map(lambda x: (x[0], x[1].replace('\\\'',"'")))\
    .map(lambda x: (x[0], re.sub('[{}+=~*%#$@(\-/[,.!?&:;\]0-9)"]', ' ', str(x[1]).lower()))) \
    .mapValues(lambda x: x.split())

total_words_num = b_text.flatMap(lambda x: x[1]).count()
rare_words = b_text.flatMap(lambda x: x[1])\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x+y)\
    .filter(lambda x: x[1] < total_words_num * 0.000001)\
    .map(lambda x: (x[0], 1))
b_unset_words = b_text.flatMap(lambda x: [(word, x[0]) for word in x[1]])\
    .subtractByKey(rare_words)\
    .subtractByKey(stopwords)
n = b_unset_words.groupByKey()\
    .map(lambda x: (x[0], len(set(x[1]))))

n_dict = dict(n.collect())
N = b_text = lines.map(lambda x: (x['business_id'])).distinct().count()

b_profile = b_unset_words.map(lambda x: (x[1], x[0]))\
    .groupByKey().map(lambda x: (x[0], list(x[1])))\
    .map(lambda x: (x[0], tf_idf(x[1]))) \
    .map(lambda x: (x[0], [word[0] for word in x[1]]))

words_list = b_profile.flatMap(lambda x: x[1]).distinct().collect()
words = dict([(word, ind) for ind, word in enumerate(words_list)])

b_profile2 = b_profile.map(lambda x: (x[0], [words[word_ind] for word_ind in x[1]]))
b_profile_dict = dict(b_profile2.collect())


def user_prof(b_list):
    u_profile_words =[]
    for b in b_list:
        u_profile_words.extend(b_profile_dict[b])
    return list(set(u_profile_words))


user_profile = lines.map(lambda x: (x['user_id'], x['business_id']))\
    .groupByKey().map(lambda x: (x[0], list(x[1])))\
    .map(lambda x: (x[0], user_prof(x[1])))
f = open(model_file, "w")
for user, u_vector in dict(user_profile.collect()).items():
    f.write(json.dumps({"id": user, "type": "user", "vector": u_vector}))
    f.write('\n')
for business, b_vector in b_profile_dict.items():
    f.write(json.dumps({"id": business, "type": "business", "vector": b_vector}))
    f.write('\n')

end_time = datetime.now()
duration = end_time - start_time
print("Duration:", duration)
