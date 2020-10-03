from pyspark import SparkContext
from sys import argv
from itertools import combinations, chain
import datetime

start_time = datetime.datetime.now()
filter_threshold = int(argv[1])
input_file = argv[2]
betweenness_output_file = argv[3]
community_output_file = argv[4]
sc = SparkContext(appName='hw4')
lines = sc.textFile(input_file)


def correlated_vertices(v_v_list):
    if None in v_v_list:
        v_v_list.remove(None)
        correlated_v = v_v_list[0]
    else:
        v_v_list[0].extend(v_v_list[1])
        correlated_v = v_v_list[0]
    return correlated_v

# Betweenness Calculation
def girvan_newman_betweenness(correlated_v_dict):
    betweenness = None
    conmunities = []
    for root in correlated_v_dict.keys():
        # construct bfs tree
        i = 0
        bfs_tree = dict()
        tree_level = dict()
        bfs_tree[root] = {'parents': [], 'children': correlated_v_dict[root], "mark": 1}
        tree_level[0] = [root]

        while (True):
            if bfs_tree[root]['children'] == []:
                break

            i += 1
            tree_level[i] = []
            next_level = []
            for p_node in tree_level[i - 1]:
                for p_child in bfs_tree[p_node]['children']:

                    if p_child not in bfs_tree.keys():
                        bfs_tree[p_child] = {'parents': [p_node], 'children': [], 'mark': bfs_tree[p_node]['mark']}
                        tree_level[i].append(p_child)
                    else:
                        bfs_tree[p_child]['parents'].append(p_node)
                        bfs_tree[p_child]['mark'] = bfs_tree[p_child]['mark'] + bfs_tree[p_node]['mark']
            for node in tree_level[i]:
                children1 = set(correlated_v_dict[node]) - set(tree_level[i])
                children2 = children1 - set(bfs_tree[node]['parents'])
                bfs_tree[node]['children'].extend(list(children2))
                next_level.extend(bfs_tree[node]['children'])
            if next_level == []:
                break

        if root not in list(chain.from_iterable(conmunities)):
            conmunities.append(list(chain.from_iterable(tree_level.values())))

        between = {}
        v_values = {}
        # calculate betweenness
        for level in sorted(tree_level, reverse=True):
            for node in tree_level[level]:
                if bfs_tree[node]['children'] == []:
                    v_values[node] = 1
                    if len(bfs_tree[node]['parents']) == 1:
                        between[tuple(sorted((bfs_tree[node]['parents'][0], node)))] = v_values[node]
                    else:
                        for p in bfs_tree[node]['parents']:
                            between[tuple(sorted((p, node)))] = bfs_tree[p]['mark'] / bfs_tree[node]['mark'] * v_values[
                                node]
                else:
                    v_values[node] = 1
                    for child in bfs_tree[node]['children']:
                        v_values[node] = v_values[node] + between[tuple(sorted((node, child)))]
                        if len(bfs_tree[node]['parents']) == 1:
                            between[tuple(sorted((bfs_tree[node]['parents'][0], node)))] = v_values[node]
                        else:
                            for p in bfs_tree[node]['parents']:
                                between[tuple(sorted((p, node)))] = bfs_tree[p]['mark'] / bfs_tree[node]['mark'] * v_values[
                                    node]
        if betweenness == None:
            betweenness = between
        else:
            for k, v in between.items():
                if k in betweenness.keys():
                    betweenness[k] += v
                else:
                    betweenness[k] = v
    for k, v in betweenness.items():
        betweenness[k] = v / 2
    betweenness = sorted(betweenness.items(), key=lambda d: (-d[1], d[0]))
    return betweenness, conmunities


# Community Detection
def community_detection(correlated_v_dict, k_dict, adj_matrix, m):
    q_commu = {}
    while(True):
        # Remove edge with the maximum betweenness each time
        betweenness_communities = girvan_newman_betweenness(correlated_v_dict)
        betweenness = betweenness_communities[0]
        if betweenness == []:
            break
        max_betweenness = betweenness[0]
        # print(max_betweenness)
        correlated_v_dict[max_betweenness[0][0]].remove(max_betweenness[0][1])
        correlated_v_dict[max_betweenness[0][1]].remove(max_betweenness[0][0])
        # Calculate Modularity
        real_expect_sum = []
        communities = betweenness_communities[1]
        for community in communities:
            pair1 = [tuple(sorted(pa)) for pa in list(combinations(community, 2))]
            for i_j in pair1:
                real_expect = adj_matrix[i_j] - ((k_dict[i_j[0]] * k_dict[i_j[1]] )/ (m * 2))
                real_expect_sum.append(real_expect)
        q = float(sum(real_expect_sum) / m)
        q_commu[q] = communities
    return q_commu[max(q_commu.keys())]
        # if q + 0.05 < q_prior:
        #     return communities
        # else:
        #     q_prior = q

        # if len(communities) == 21:
        #     print(q)
        #     return communities

        # if len(communities)==25:




header = lines.first()
lines = lines.map(lambda x: x.split(',')) \
    .filter(lambda x: x[0] != header) \
    .map(lambda x: (x[1], x[0])) \
    .groupByKey().map(lambda x: (x[0], list(x[1]))) \
    .map(lambda x: [(tuple(sorted(u_u)), 1) for u_u in list(combinations(x[1], 2))]) \
    .flatMap(lambda x: x) \
    .reduceByKey(lambda x, y: x + y) \
    .filter(lambda x: x[1] >= filter_threshold) \
    .map(lambda x: x[0])
v_pair1 = lines.groupByKey().map(lambda x: (x[0], list(x[1])))
v_pair2 = lines.map(lambda x: (x[1], x[0])) \
    .groupByKey().map(lambda x: (x[0], list(x[1])))
v_pair = v_pair1.fullOuterJoin(v_pair2) \
    .map(lambda x: (x[0], correlated_vertices(list(x[1]))))
correlated_v_dict = dict(v_pair.collect())
betweenness_origin = girvan_newman_betweenness(correlated_v_dict)[0]
# print(betweenness_origin)
with open(betweenness_output_file, 'w') as f:
    for item in betweenness_origin:
        f.write(str(item[0]) + ', ' + str(item[1]) + '\n')
k_dict = dict(v_pair.map(lambda x: (x[0], len(x[1]))).collect())
adj_matrix = dict()
for pair in list(combinations(correlated_v_dict.keys(), 2)):
    if pair[1] in correlated_v_dict[pair[0]]:
        adj_matrix[tuple(sorted(pair))] = 1
    else:
        adj_matrix[tuple(sorted(pair))] = 0
m = len(betweenness_origin)


commu = community_detection(correlated_v_dict, k_dict, adj_matrix, m)
# print(commu)
commu = [sorted(community) for community in commu]
with open(community_output_file, 'w') as f:
    for community in sorted(commu, key=lambda x: (len(x), x[0])):
        f.write(str(community).strip('[]') + '\n')

end_time = datetime.datetime.now()
duration = end_time - start_time
print("Duration:", duration)
