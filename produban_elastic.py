from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import MultiSearch, Search
from time import time
from time import sleep
import json
from elasticsearch_dsl.response import Response

client = Elasticsearch(hosts=['localhost:9256'])
# Define a default Elasticsearch client
# connections.create_connection(hosts=['localhost:9212'])

# dumped_query = json.dumps({'query': {
#         'match': {'_type': 'barebones_connections'},
#         'match': {'source_ip': str(first_ip_addr)},
#         'match': {'destination_ip': str(second_ip_addr)}
#     }})
#
# base_result = Elasticsearch.search(body=dumped_query, size=200)['hits']['hits']

# Single Search
# equivalent pagination: s = Search().query(...).extra(from_=0, size=25)
s = Search(using=client) \
    .filter("match", _type="barebones_connections")[0:10] \
    .filter("match", source_ip="193.238.46.51") \
    .filter("match", destination_port="22") \
    .filter("range", slot_start_time={"gte": "now-12d", "lt": "now"})
    # alternative:   slot_start_time={"gte": "2018-11-05T14:00:00", "lt": "now"})
    # alternative:   slot_start_time={"gte": "2018-11-05T14:00:00", "lt": "2018-11-05T14:59:59"})

# for hit in s.scan():
#     print(hit)

# Multi Search
ms = MultiSearch(using=client)
ms = ms.add(
    Search().filter("match", _type="barebones_connections")
            .filter('match', source_ip="193.238.46.51")
            .filter('match', destination_port="22"))

ms = ms.add(Search().filter('match', source_ip="193.238.46.51"))
ms = ms.add(Search().filter('match', destination_port="22"))

# responses = ms.execute()

response = s.execute()
print(len(response.hits))
print(response.hits.hits)
# print(responses)

# for hit in response:
#     print(hit)


####################
# produban details #
####################

# s = Search(using=client) \
#     .filter("match", _type="barebones_connections") \
#     .filter("match", source_ip="192.168.115.6") \
#     .filter("match", destination_port="445") \
#     .filter("range", slot_start_time={"gte": "2018-10-02T10:00:00", "lt": "2018-10-02T11:00:00"})[:100000]
# s.aggs.bucket('per_dest_ip', 'terms', field='destination_ip', size=100000)
# s.aggs.bucket('distinct_dest_ip', 'cardinality', field='destination_ip')

# To find only distinct --> use aggregations
# s.aggs.bucket('bucket_name', 'terms', field='field_to_filter_by', size=Num) -- to find repeated terms with count,
# increase num to return more results
#
# s.aggs.bucket('distinct_dest_ip', 'cardinality', field='destination_ip')    -- to find count of distinct


def execute_query(q: Search):
    """
    Executes the query of the given Search object, and returns the result along with the
    time taken to execute it.

    :param q: Search object containing the query to be executed
    :return: List containing the query result and the time taken to execute it [queryResult, executionTime]
    """
    startTime = time()
    res = q.execute()
    endTime = time()
    execTime = endTime - startTime
    return [res, execTime]


def search_by_src_ip(src_ip):
    """
    Returns the matching barebones_connections for the given source ip on port 445 during the interval defined
    within the function.

    :param src_ip: The source ip to search by
    :return: Response object of barebones_connections to port 445 where the source ip is the given one
    """

    searchq = Search(using=client) \
        .filter("match", _type="barebones_connections") \
        .filter("match", source_ip=src_ip) \
        .filter("match", destination_port="445") \
        .filter("range", slot_start_time={"gte": "2018-10-02T10:00:00", "lt": "2018-10-10T10:00:00"})[:100000]

    searchq.aggs.bucket('per_dest_ip', 'terms', field='destination_ip', size=100000)
    searchq.aggs.bucket('per_dest_process', 'terms', field='destination_process', size=100000)

    response = searchq.execute()

    return response


def search_by_src_ips(ip_list, start, amount):
    """
    Searches the matching barebones_connections for the given amount of source ip addresses on port 445,
    starting from the index given by start. The range of the search is the interval defined within the function.

    :param ip_list: List of source ip addresses to search by
    :param start: The starting index of the source ips
    :param amount: The amount of ips to search from the starting index
    :return: Two lists. The first contains the Response objects for each one of the searched source ips, while the
    second contains the length of each response result. In addition, the total time taken to execute the query
    is also returned.
    """

    responses = []
    responses_len = []

    startTime = time()

    for index in range(start, min(len(ip_list), (start + amount))):
        responses.append(search_by_src_ip(ip_list[index]))
        responses_len.append(responses[-1].hits.total)
        if responses_len[-1] == 0:
            responses.pop()
            responses_len.pop()

    endTime = time()

    return [responses, responses_len, (endTime - startTime)]


# [tier2Res, tier2ResLen, execTime] = search_by_src_ips(srcIps, 0, 100)


#### Network Events Queries
###########################

# searchq = Search(using=client) \
#         .filter("match", _type="network_events")
# searchq.aggs.bucket("per_event_type", "terms", field="event_type", size=100000)


def extract_subnets(ip_list, resolution=24):
    """
    Extracts the subnet up to the given resolution of the ip addresses in the given list

    :param ip_list: List of input ip addresses to extract the subnet from
    :param resolution: resolution of the returned subnets (8, 16, or 24)
    :return: List of extracted subnets from the input ip addresses.
    """

    subnets = set()
    for ip in ip_list:
        subnet_parts = ip.split(".")
        if resolution == 8:
            subnets.add(subnet_parts[0] + ".0.0.0")
        elif resolution == 16:
            subnets.add(".".join(subnet_parts[:2]) + ".0.0")
        elif resolution == 24:
            subnets.add(".".join(subnet_parts[:3]) + ".0")
        else:
            return None

    return subnets


def more_than_x_dsts(response: Response, num_of_dsts=3):
    """
    Returns the source ip address of the given response if it has connections to distinct destination ips of a number
    which is greater than or equal to the input one.
    *VIP* name the aggregation grouping the destination ips as "per_dest_ip"
    --> use search_query.aggs.bucket('per_dest_ip', 'terms', field='destination_ip', size=100000)

    :param response: response to check
    :param num_of_dsts: threshold number of destination ips to consider malicious
    :return: src_ip if the response matches the criteria, otherwise False
    """

    if len(response.aggregations.per_dest_ip.buckets._l_) > num_of_dsts:
        return response.hits.hits[0]["_source"]["source_ip"]

    return False


def specific_dst_process(response: Response, process_name):
    """
    Returns the source ip address of the given response if it has connections to the given destination process.

    *VIP* name the aggregation grouping the destination ips as "per_dest_process"
    --> use search_query.aggs.bucket('per_dest_process', 'terms', field='destination_process', size=100000)

    :param response: response to check
    :param process_name: the name of the destination process to look for
    :return: src_ip if the response matches the criteria, otherwise False
    """

    destination_processes = [dict["key"] for dict in response.aggregations.per_dest_process.buckets._l_]

    for dest_process in destination_processes:
        if process_name in dest_process:
            return response.hits.hits[0]["_source"]["source_ip"]

    return False
