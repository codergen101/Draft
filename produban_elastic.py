from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import MultiSearch, Search
import json

client = Elasticsearch()
# Define a default Elasticsearch client
connections.create_connection(hosts=['localhost'])

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
        .filter("match", _type="barebones_connections")[0:10]

# for hit in s.scan():
#     print(hit)

# Multi Search
ms = MultiSearch(using=client)
ms = ms.add(Search().filter("match", _type="barebones_connections").filter('match', source_ip="193.238.46.51").filter('match', destination_port="22"))
ms = ms.add(Search().filter('match', source_ip="193.238.46.51"))
ms = ms.add(Search().filter('match', destination_port="22"))

# responses = ms.execute()

response = s.execute()
print(len(response.hits))
print(response.hits.hits)
# print(responses)

# for hit in response:
#     print(hit)
