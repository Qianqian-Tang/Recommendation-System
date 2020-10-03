from pyspark import SparkContext
from sys import argv
import os
import datetime
import random
import math
from itertools import combinations, chain
import json
import csv

start_time = datetime.datetime.now()
data_folder = argv[1]
n_cluster = int(argv[2])
out_file1 = argv[3]
out_file2 = argv[4]
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.7'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.7'
sc = SparkContext(appName='hw5')


def euclidean_dist(point1, point2):
    pow_vector = []
    d = len(point1)
    for i in range(d):
        pow_vector.append(math.pow((point1[i] - point2[i]), 2))
    sum_vector = sum(pow_vector)
    euc_dist = math.sqrt(sum_vector)
    return euc_dist


def assign_points(data_point, centroids):
    euc_dis = dict()
    for cluster_id, centroid_value in centroids.items():
        euc_dis[cluster_id] = euclidean_dist(data_point, centroid_value)
    return min(euc_dis, key=euc_dis.get)
    # return assigned cluster_id


def find_new_centroids(dataid_value):
    # points are in the same cluster by previous centroids
    new_centroid = []
    d = len(dataid_value[0][1])
    n_points = len(dataid_value)
    for i in range(d):
        new_centroid_compo = []
        for point in dataid_value:
            new_centroid_compo.append(point[1][i])
        new_centroid.append(sum(new_centroid_compo) / n_points)
    data_ids = [id_value[0] for id_value in dataid_value]
    return new_centroid, data_ids
def summarize(data_indice, data):
    points = [data[ind] for ind in data_indice]
    represent = []
    dimen = len(points[0])
    n = len(points)
    represent.append(n)
    point_sum = []
    point_sum_square = []
    for i in range(dimen):
        point_i = []
        point_i_sq = []
        for point in points:
            point_i.append(point[i])
            point_i_sq.append(math.pow(point[i], 2))
        point_sum.append(sum(point_i))
        point_sum_square.append(sum(point_i_sq))
    represent.extend(point_sum)
    represent.extend(point_sum_square)

    return represent
def kmeans(data, data_points, id_start, id_end, n_iteration):

    # initial centroids
    indice = list(data.keys())
    new_centroid = random.choice(indice)
    centroids = dict()
    t = 0
    dist_sort = []

    # centroid = data.pop(first_random_point)
    # dimension = len(centroid)
    centroids[0] = data[new_centroid]
    for cluster_id in range(id_start, id_end):
        # print("new_centroid: ", new_centroid)
        indice.remove(new_centroid)
        point_dists = []
        for point in indice:
            euc_dist = euclidean_dist(data[new_centroid], data[point])
            point_dists.append((point, euc_dist))
        if len(point_dists) >= id_end:
            point_dists = sorted(point_dists, key=lambda x: x[1], reverse=True)[id_end:]
        else:
            point_dists = sorted(point_dists, key=lambda x: x[1], reverse=True)

            # point_dists =
        dist_sort.extend(point_dists)
        dist_sort = sorted(point_dists, key=lambda x: x[1], reverse=True)
        for p_d in dist_sort:
            if p_d[0] in centroids.keys():
                continue
            else:
                new_centroid = p_d[0]
                centroids[cluster_id] = data[new_centroid]

    # find k-means centroids
    for i in range(n_iteration):
        new_centroids = data_points.map(lambda x: (assign_points(x[1], centroids), (x[0], x[1]))) \
            .groupByKey().map(lambda x: (x[0], list(x[1]))) \
            .map(lambda x: (x[0], find_new_centroids(x[1]))) \
            .map(lambda x: (x[0], x[1][0])).collect()
        centroids = dict(new_centroids)
        if i == n_iteration - 1:
            clusters = data_points.map(lambda x: (assign_points(x[1], centroids), (x[0], x[1]))) \
                .groupByKey().map(lambda x: (x[0], list(x[1]))) \
                .map(lambda x: (x[0], find_new_centroids(x[1]))).collect()
            # centroids = [centroid[0] for centroid in clusters]
            # (assigned cluster_id, (dataid, data_value))
            # (cluster_id, [(data_id, data_value), (data_id, data_value)...])
            # return [(cluster id, (new new_centroid value, [data_ids...]))，...]

            return clusters
def mahalanobis_dist(point, set):
    # point: value
    # set: [cluster id, cluster center, summarization]
    d = len(point)
    md_sum = []
    for i in range(d):
        # print(set[2][i+d+1])

        variance = ((set[2][i+d+1] / set[2][0]) - math.pow((set[2][i+1] / set[2][0]), 2))
        sigma = math.sqrt(variance)
        x_subtract_c_divide_sigma = (point[i] - set[1][i]) / sigma
        md_pow = math.pow(x_subtract_c_divide_sigma, 2)
        md_sum.append(md_pow)
    md_summa = sum(md_sum)
    mahala_d = math.sqrt(md_summa)
    return mahala_d
def merge_cs(cs_with_cent, cs_result_dict):
    # cs_with_cent: cluster id, cluster center, summarization
    cs_with_cent1 = cs_with_cent
    for cs_cluster_pair in list(combinations(cs_with_cent, 2)):
        if cs_cluster_pair[0] not in cs_with_cent1 or cs_cluster_pair[1] not in cs_with_cent1:
            continue
        else:
            mh_dist = mahalanobis_dist(cs_cluster_pair[0][1], cs_cluster_pair[1])
            if mh_dist < 2 * math.sqrt(len(v)):
                new_cs_cluster = []
                new_cs_summari = []
                new_center = []
                for i in range(len(cs_cluster_pair[0][2])):
                    new_cs_summari.append(cs_cluster_pair[0][2][i]+cs_cluster_pair[1][2][i])
                for d in range(len(cs_cluster_pair[0][1])):
                    new_center_d = (cs_cluster_pair[0][1][d] + cs_cluster_pair[1][1][d]) / 2
                    new_center.append(new_center_d)
                new_cs_cluster.append(cs_cluster_pair[0][0])
                new_cs_cluster.append(new_center)
                new_cs_cluster.append(new_cs_summari)
                cs_with_cent1.append(new_cs_cluster)
                cs_with_cent1.remove(cs_cluster_pair[0])
                cs_with_cent1.remove(cs_cluster_pair[1])
                # change cs_result_dict
                # cs_result_dict: {cluster id: [data_ids]}
                cs_result_dict[cs_cluster_pair[0][0]].extend(cs_result_dict[cs_cluster_pair[1][0]])
                cs_result_dict.pop(cs_cluster_pair[1][0])

    return cs_with_cent1, cs_result_dict


for root, dirs, files in os.walk(data_folder):
    round_list = []
    header = ["round_id", "nof_cluster_discard", "nof_point_discard", "nof_cluster_compression", "nof_point_compression", "nof_point_retained"]
    round_list.append(header)
    for file_id, file in enumerate(files):
        if file_id == 0:
            init = sc.textFile(os.path.join(root, file)).sample(False, 0.05)\
                .map(lambda x: x.split(','))\
                .map(lambda x: (x[0], [float(component) for component in x[1:]]))
            data = dict(init.collect())
            data2 = dict(init.collect())
            data3 = data2.copy()
            # remove outliers
            clusters = kmeans(data, init, 1, n_cluster * 5, 1)

            for clus in clusters:
                if len(clus[1][1]) < (len(data) / 10000):
                    for id in clus[1][1]:
                        data3.pop(id)

            clusters = kmeans(data3, init, 1, n_cluster * 3, 12)
            # def kmeans(data, data_points, id_start, id_end):
            sorted_cluster_ds = sorted(clusters, key=lambda x: len(x[1][1]), reverse=True)[:n_cluster]
            ds_result_dict = {}
            for ds_cluster in sorted_cluster_ds:
                for data_id in ds_cluster[1][1]:
                    ds_result_dict[data_id] = ds_cluster[0]
            total = []
            # for c in clusters:
            #     total.append(len(c[1][1]))
            #     print("clusters", len(c[1][1]))
            # print("sum clusters data: ", sum(total))
            # test = []
            # for c in sorted_cluster_ds:
            #     test.append(len(c[1][1]))
            #     print("ds_cluster", len(c[1][1]))
            # print("sum ds data: ", sum(test))
            # [(cluster id, (new new_centroid value, [data_ids...]))...]
            sorted_cluster_cs_rs = sorted(clusters, key=lambda x: len(x[1][1]), reverse=True)[n_cluster:]
            cs_result_dict = {}
            ds_with_cent = []
            cs_with_cent = []
            rs = {}
            for cent_ds_cluster in sorted_cluster_ds:
                ds_with_cent.append((cent_ds_cluster[0], cent_ds_cluster[1][0], summarize(cent_ds_cluster[1][1], data2)))
            for cent_cs_cluster in sorted_cluster_cs_rs:
                if len(cent_cs_cluster[1][1]) > 1:
                    # print("len cs: ", len(cent_cs_cluster[]))
                    # cs_result_dict: {cluster id: [data_ids]}
                    cs_result_dict[cent_cs_cluster[0]] = cent_cs_cluster[1][1]
                    cs_with_cent.append((cent_cs_cluster[0], cent_cs_cluster[1][0], summarize(cent_cs_cluster[1][1], data2)))
                else:
                    rs[cent_cs_cluster[1][1][0]] = data[cent_cs_cluster[1][1][0]]
            # ds_with_cent: cluster id, cluster center, summarization
            # cs_with_cent: cluster id, cluster center, summarization
            # rs: rs points

            ds = []
            cs = []
            data_points = sc.textFile(os.path.join(root, file)) \
                .map(lambda x: x.split(',')) \
                .map(lambda x: (x[0], [float(component) for component in x[1:]]))
            data_points.subtractByKey(init)
            indice = data_points.map(lambda x: x[0]).collect()
            data = dict(data_points.collect())
            for ind, v in list(data.items()):
                mahala_d_list = []
                for ds_cluster in ds_with_cent:
                    mahala_d = mahalanobis_dist(v, ds_cluster)
                    mahala_d_list.append((ds_cluster[0], mahala_d))
                sorted_mh = sorted(mahala_d_list, key=lambda x: x[1])
                if sorted_mh[0][1] < 2 * math.sqrt(len(v)):
                    ds.append((ind, sorted_mh[0][0]))
                    # ds: [(data id, cluster id), ...]
                    data.pop(ind)
            ds_result_dict.update(dict(ds))
            # cs_with_cent: cluster id, cluster center, summarization
            for ind, v in list(data.items()):
                mahala_d_list = []
                for set in cs_with_cent:
                    mahala_d = mahalanobis_dist(v, set)
                    mahala_d_list.append((set[0], mahala_d))
                sorted_mh = sorted(mahala_d_list, key=lambda x: x[1])
                # sorted_mh: (cluster id, mh_dist)
                if sorted_mh[0][1] < 3 * math.sqrt(len(v)):
                    cs.append((sorted_mh[0][0], ind))
                    # cs: [(cluster id, data id), ...]
                    data.pop(ind)
            # cs_result_dict.update(dict(cs))
            for tup in cs:
                if tup[0] not in cs_result_dict.keys():
                    cs_result_dict[tup[0]] = []
                    cs_result_dict[tup[0]].append(tup[1])
                else:
                    cs_result_dict[tup[0]].append(tup[1])
                    # cs_result_dict: {cluster id: [data_ids]}
            rs.update(data)
            # merge rs to cs and rs
            if rs:
                data2 = data
                # rs is dict here
                rs_list = []
                for k, v in rs.items():
                    rs_list.append((k, v))
                data_points = sc.parallelize(rs_list)
                if len(rs) >= n_cluster:
                    rs_merged_cs = kmeans(rs, data_points, 1 + n_cluster, n_cluster + len(rs), 15)
                    # return [(cluster id, (new new_centroid value, [data_ids...]))，...]
                    # print(rs_merged_cs)
                    for cluster in rs_merged_cs:
                        if len(cluster[1][1]) > 1:
                            # cs_with_cent.append(cluster)
                            # for data_id in cluster[1][1]:
                            cs_result_dict[cluster[0]] = cluster[1][1]
                            cs_with_cent.append((cluster[0], cluster[1][0],
                                                 summarize(cluster[1][1], data2)))
                            # cs: [(cluster id, data id), ...]
                            for data_id in cluster:
                                cs.append((cluster[0], data_id))
                                rs.pop(data_id)

            # merge cs
            merged_cs = merge_cs(cs_with_cent, cs_result_dict)
            cs_with_cent = merged_cs[0]
            cs_result_dict = merged_cs[1]



            round_line = []
            round_id = file_id + 1
            nof_cluster_discard = len(ds_with_cent)
            nof_point_discard = len(ds_result_dict)
            nof_cluster_compression = len(cs_with_cent)
            nof_point_compression = len(list(chain.from_iterable(cs_result_dict.values())))
            nof_point_retained = len(rs)
            round_line.append(round_id)
            round_line.append(nof_cluster_discard)
            round_line.append(nof_point_discard)
            round_line.append(nof_cluster_compression)
            round_line.append(nof_point_compression)
            round_line.append(nof_point_retained)
            round_list.append(round_line)
        else:
            ds = []
            cs = []
            data_points = sc.textFile(os.path.join(root, file)) \
                .map(lambda x: x.split(',')) \
                .map(lambda x: (x[0], [float(component) for component in x[1:]]))
            indice = data_points.map(lambda x: x[0]).collect()
            data = dict(data_points.collect())
            for ind, v in list(data.items()):
                mahala_d_list = []
                for ds_cluster in ds_with_cent:
                    mahala_d = mahalanobis_dist(v, ds_cluster)
                    mahala_d_list.append((ds_cluster[0], mahala_d))
                sorted_mh = sorted(mahala_d_list, key=lambda x: x[1])
                if sorted_mh[0][1] < 2 * math.sqrt(len(v)):
                    ds.append((ind, sorted_mh[0][0]))
                    # ds: [(data id, cluster id), ...]
                    data.pop(ind)
            ds_result_dict.update(dict(ds))
            # cs_with_cent: cluster id, cluster center, summarization
            for ind, v in list(data.items()):
                mahala_d_list = []
                for set in cs_with_cent:
                    mahala_d = mahalanobis_dist(v, set)
                    mahala_d_list.append((set[0], mahala_d))
                sorted_mh = sorted(mahala_d_list, key=lambda x: x[1])
                # sorted_mh: (cluster id, mh_dist)
                if sorted_mh[0][1] < 3 * math.sqrt(len(v)):
                    cs.append((sorted_mh[0][0], ind))
                    # cs: [(cluster id, data id), ...]
                    data.pop(ind)
            # cs_result_dict.update(dict(cs))
            for tup in cs:
                if tup[0] not in cs_result_dict.keys():
                    cs_result_dict[tup[0]] = []
                    cs_result_dict[tup[0]].append(tup[1])
                else:
                    cs_result_dict[tup[0]].append(tup[1])
                    # cs_result_dict: {cluster id: [data_ids]}
            rs.update(data)
            # merge rs to cs and rs
            if rs:
                data2 = data
                # rs is dict here
                rs_list = []
                for k, v in rs.items():
                    rs_list.append((k, v))
                data_points = sc.parallelize(rs_list)
                if len(rs) >= n_cluster:
                    rs_merged_cs = kmeans(rs, data_points, 1 + n_cluster, n_cluster + len(rs), 15)
                    # return [(cluster id, (new new_centroid value, [data_ids...]))，...]
                    # print(rs_merged_cs)
                    for cluster in rs_merged_cs:
                        if len(cluster[1][1]) > 1:
                            # cs_with_cent.append(cluster)
                            # for data_id in cluster[1][1]:
                            cs_result_dict[cluster[0]] = cluster[1][1]
                            cs_with_cent.append((cluster[0], cluster[1][0],
                                                 summarize(cluster[1][1], data2)))
                            # cs: [(cluster id, data id), ...]
                            for data_id in cluster:
                                cs.append((cluster[0], data_id))
                                rs.pop(data_id)




            # merge cs
            merged_cs = merge_cs(cs_with_cent, cs_result_dict)
            cs_with_cent = merged_cs[0]
            cs_result_dict = merged_cs[1]
            if file == files[-1]:
                # print("last round!")
                # last round, merge cs and rs to ds
                # cs_with_cent: cluster id,
                # cluster center, summarization
                for cs_cluster in cs_with_cent:
                    mahala_d_list = []
                    cs_with_cent = cs_with_cent.copy()
                    for set in ds_with_cent:
                        mahala_d = mahalanobis_dist(cs_cluster[1], set)
                        mahala_d_list.append((set[0], mahala_d))
                    sorted_mh = sorted(mahala_d_list, key=lambda x: x[1])
                    for data_id in cs_result_dict[cs_cluster[0]]:
                        cs_result_dict = cs_result_dict.copy()
                        ds_result_dict[data_id] = sorted_mh[0][0]
                    cs_result_dict.pop(cs_cluster[0])
                    cs_with_cent.remove(cs_cluster)
                if rs:
                    for id, value in list(rs.items()):
                        mahala_d_list = []
                        for set in ds_with_cent:
                            mahala_d = mahalanobis_dist(value, set)
                            mahala_d_list.append((set[0], mahala_d))
                        sorted_mh = sorted(mahala_d_list, key=lambda x: x[1])
                        ds_result_dict[id] = sorted_mh[0][0]
                        rs.pop(id)
                round_line = []
                round_id = file_id + 1
                nof_cluster_discard = len(ds_with_cent)
                nof_point_discard = len(ds_result_dict)
                nof_cluster_compression = len(cs_with_cent)
                nof_point_compression = len(list(chain.from_iterable(cs_result_dict.values())))
                nof_point_retained = len(rs)
                round_line.append(round_id)
                round_line.append(nof_cluster_discard)
                round_line.append(nof_point_discard)
                round_line.append(nof_cluster_compression)
                round_line.append(nof_point_compression)
                round_line.append(nof_point_retained)
                round_list.append(round_line)
                with open(out_file2, 'w') as csv_file:
                    round_writer = csv.writer(csv_file, delimiter=',')
                    for line in round_list:
                        round_writer.writerow(line)
                with open(out_file1, 'w') as json_file:
                    json.dump(ds_result_dict, json_file)
                break
            # cs_result_dict: {cluster id: [data_ids]}
            round_line = []
            round_id = file_id + 1
            nof_cluster_discard = len(ds_with_cent)
            nof_point_discard = len(ds_result_dict)
            nof_cluster_compression = len(cs_with_cent)
            nof_point_compression = len(list(chain.from_iterable(cs_result_dict.values())))
            nof_point_retained = len(rs)
            round_line.append(round_id)
            round_line.append(nof_cluster_discard)
            round_line.append(nof_point_discard)
            round_line.append(nof_cluster_compression)
            round_line.append(nof_point_compression)
            round_line.append(nof_point_retained)
            round_list.append(round_line)
end_time = datetime.datetime.now()
duration = end_time - start_time
print("Duration:", duration)
