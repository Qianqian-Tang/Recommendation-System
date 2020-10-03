from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from sys import argv
import datetime
import binascii
import json
import math
start_time = datetime.datetime.now()
port = int(argv[1])
output_file = argv[2]


def hash_to_binary(city):
    bina = []
    city_num = int(binascii.hexlify(city.encode('utf8')), 16)
    for a in range(1, 80, 2):
        for b in range(1, 17, 7):
            index = ((a * city_num + b) % 149399) % 3477
            bi = bin(index)
            bina.append(bi)
    return bina


def flajolet_martin(window):
    datatime_now = datetime.datetime.now()
    timestamp = datatime_now - datetime.timedelta(microseconds=datatime_now.microsecond)
    cities = window.collect()
    ground_truth = len(set(cities))
    print("ground:", ground_truth)
    hash_list = []
    esimate_counts =[]
    for city in cities:
        hash_values = hash_to_binary(city)
        hash_list.append(hash_values)
    print("hash: ", len(hash_list))
    for hash_i in range(len(hash_list[0])):
        for hash_values in hash_list:
            city_binary = str(hash_values[hash_i])
            n_zero = 0
            max_zero = 0
            for i in range(1, len(city_binary) + 1):
                if city_binary[-i] == "0":
                    n_zero += 1
            if n_zero > max_zero:
                max_zero = n_zero
            esimate_count_i = math.pow(2, max_zero)
        esimate_counts.append(esimate_count_i)
    print(len(esimate_counts))
    median_counts = []
    for i in range(0, 120, 6):
        mean_count = []
        mean_count.extend([esimate_counts[i], esimate_counts[i + 1], esimate_counts[i + 2]])
        mean = sum(mean_count) / len(mean_count)
        median_counts.append(mean)
    m = len(median_counts)
    median_counts.sort()
    if m % 2 == 0:
        median1 = median_counts[m // 2]
        median2 = median_counts[m // 2 - 1]
        mean_count = (median1 + median2) / 2
    else:
        mean_count = median_counts[m // 2]
    f.write(str(timestamp) + "," + str(ground_truth) + "," + str(mean_count))
    f.write("\n")
    f.flush()


sc = SparkContext("local[2]", "flajolet_martin")
ssc = StreamingContext(sc, 5)
ssc.checkpoint("check")
lines = ssc.socketTextStream("localhost", port)
f = open(output_file, 'w+')
f.write("Time,Ground Truth,Estimation")
f.write("\n")
city = lines.map(lambda x: json.loads(x))\
    .map(lambda x: x['city'])\
    .filter(lambda x: x != "")\
    .window(30, 10)\
    .foreachRDD(flajolet_martin)
ssc.start()
ssc.awaitTermination()

end_time = datetime.datetime.now()
duration = end_time - start_time
print("Duration:", duration)
