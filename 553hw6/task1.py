from pyspark import SparkContext
from sys import argv
import datetime
import binascii
import json
import math
start_time = datetime.datetime.now()
first_json_path = argv[1]
second_json_path = argv[2]
output_file_path = argv[3]
sc = SparkContext(appName='hw6')


def hash_city(city, k, n):
    i = 0
    indice = []
    for a in range(1, 100, 7):
        if i > k:
            break
        for b in range(1, 20, 7):
            i += 1
            if i > k:
                break
            index = ((a * city + b) % 149399) % n
            indice.append(index)
    return indice


def check_ind(hash_ind, bit_array):
    for ind in hash_ind:
        if ind in bit_array:
            continue
        else:
            return 0
    return 1


first_cities = sc.textFile(first_json_path)\
    .map(lambda x: json.loads(x))\
    .map(lambda x: x['city'])\
    .filter(lambda x: x != "")\
    .map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16))\
    .distinct()
m = first_cities.count()
n = 3000
k = int((n / m) * math.log(2))
bit_array_rdd = first_cities.map(lambda x: hash_city(x, k, n))\
    .flatMap(lambda x: x)
bit_array = bit_array_rdd.collect()

predict_cities = sc.textFile(second_json_path)\
    .map(lambda x: json.loads(x))\
    .map(lambda x: x['city'])\
    .map(lambda x: "None" if x == "" else x)\
    .map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16))\
    .map(lambda x: hash_city(x, k, n))\
    .map(lambda x: check_ind(x, bit_array))
with open(output_file_path, 'w') as csv_file:
    for num in predict_cities.collect():
        csv_file.write(str(num) + " ")
end_time = datetime.datetime.now()
duration = end_time - start_time
print("Duration:", duration)