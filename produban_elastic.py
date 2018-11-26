from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import MultiSearch, Search
from time import time
from time import sleep
import json
from elasticsearch_dsl.response import Response
import logging

client = Elasticsearch(hosts=['localhost:9256'])
# client = connections.create_connection(hosts=['localhost:9256'], timeout=1000000)
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


# Network Events Queries ##
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


# def search_by_src_ids(id_list, start, amount):
#     """
#     Searches the matching barebones_connections for the given amount of source ip addresses on port 445,
#     starting from the index given by start. The range of the search is the interval defined within the function.
#
#     :param ip_list: List of source ip addresses to search by
#     :param start: The starting index of the source ips
#     :param amount: The amount of ips to search from the starting index
#     :return: Two lists. The first contains the Response objects for each one of the searched source ips, while the
#     second contains the length of each response result. In addition, the total time taken to execute the query
#     is also returned.
#     """
#
#     global responses
#     global responses_len
#     global execTime
#     global globalIndex
#     start = globalIndex
#
#     startTime = time()
#
#     for index in range(start, min(len(id_list), (start + amount))):
#         responses.append(search_by_src_ip(ip_list[index]))
#         responses_len.append(responses[-1].hits.total)
#         if responses_len[-1] == 0:
#             responses.pop()
#             responses_len.pop()
#         with open(str(index) + ".pkl", "wb") as f:
#             temp = [responses[-1].hits._l_[0], responses[-1].aggregations.per_dest_ip.buckets._l_,
#                       responses[-1].aggregations.per_dest_process.buckets._l_]
#             pickle.dump(temp, f)
#         globalIndex += 1
#
#     endTime = time()
#
#     execTime = endTime - startTime


# searchq = Search(using=client) \
#         .filter("match", _type="barebones_connections") \
#         .filter("match", source_ip="192.168.115.6") \
#         .filter("match", destination_port="445") \
#         .filter("range", slot_start_time={"gte": "2018-10-01T00:00:00", "lt": "2018-11-01T00:00:00"})[:100000]
# searchq.aggs.bucket('per_dest_ip', 'terms', field='destination_ip', size=100000)


def csv_from_response(response):
    """

    :param response:
    :return:
    """

    ip = str(response.hits._l_[0].source_ip)
    dest_ips = '"' + ",".join([str(elem["key"]) for elem in response.aggregations.per_dest_ip.buckets._l_]) + '"'
    dest_ips_count = '"' + ",".join(
        [str(elem["doc_count"]) for elem in response.aggregations.per_dest_ip.buckets._l_]) + '"'
    dest_processes = '"' + ",".join(
        [str(elem["key"]) for elem in response.aggregations.per_dest_process.buckets._l_]) + '"'

    return ",".join([ip, dest_ips, dest_ips_count, dest_processes])


def csv_from_responses(responses_list):
    """

    :param responses_list:
    :return:
    """

    full_csv = "source_ip,dest_ips,des_ips_count,dest_process\n"

    for res in responses_list:
        full_csv += csv_from_response(res) + "\n"

    return full_csv


# Tmux commands
# --> tmux new -s ssh-tunnel
# --> tmux list-sessions
# --> tmux kill-session -t session_name
# --> tmux ls
# To detach --> Ctrl + b --> then press d


##########################
# Network Scan Incidents #
##########################

def convert_incident_time(incident_time):
    """
    Converts the time format as it appears in the JSON object of the MongoDB results to Elasticsearch format (UTC)

    :param incident_time: time to be re-formatted
    :return: YYYY-MM-DDTHH:MM:SS format (UTC) of the input incident_time
    """

    day_hyphen_index = incident_time.rfind("-")
    second_index = incident_time.rfind(":")
    incident_time = incident_time[:day_hyphen_index + 3] + "T" + incident_time[day_hyphen_index + 4:]
    incident_time = incident_time[:second_index + 3]
    return incident_time


def check_time(input_time):
    """
    Adds 0 to the beginning of the string if it is formed of single digits.

    :param input_time: time to be checked
    :return: two digits format of the input time
    """

    if len(input_time) == 1:
        return "0" + input_time
    return input_time


def window_time_range(incident_time, threshold=5, unit="second"):
    """
    Returns the start and end times of the window in which the incident process is to be searched

    :param threshold: amount of time in seconds to search for the process in the connections log (Default is 5s)
    :param incident_time: the time of the incident
    :param unit: the unit used to calculate the window. Can be "second", "minute", or "hour". Default is seconds
    :return: [start_time, end_time] of the process search window
    """

    if unit == "second":
        second_index = incident_time.rfind(":")
        seconds = int(incident_time[second_index + 1: second_index + 3])
        min_sec, max_sec = check_time(str(max(seconds - threshold, 0))), check_time(str(min(seconds + threshold, 59)))
        min_time = incident_time[:second_index + 1] + min_sec
        max_time = incident_time[:second_index + 1] + max_sec
        return [min_time, max_time]

    elif unit == "minute":
        minute_index = incident_time.find(":")
        minutes = int(incident_time[minute_index + 1: minute_index + 3])
        min_min, max_min = check_time(str(max(minutes - threshold, 0))), check_time(str(min(minutes + threshold, 59)))
        min_time = incident_time[:minute_index + 1] + min_min + incident_time[minute_index + 3:]
        max_time = incident_time[:minute_index + 1] + max_min + incident_time[minute_index + 3:]
        return [min_time, max_time]

    elif unit == "hour":
        hour_index = incident_time.find("T")
        hour = int(incident_time[hour_index + 1: hour_index + 3])
        min_hour, max_hour = check_time(str(max(hour - threshold, 0))), check_time(str(min(hour + threshold, 23)))
        min_time = incident_time[:hour_index + 1] + min_hour + incident_time[hour_index + 3:]
        max_time = incident_time[:hour_index + 1] + max_hour + incident_time[hour_index + 3:]
        return [min_time, max_time]

    return None


def get_processes_by_scan_incident_time(src_ip, incident_time, threshold_window=5, threshold_unit="second"):
    """
    Attempts to find the source process which is responsible for the scan incident by finding connections in a
    time window which is close to the given time of the incident.

    :param src_ip: IP of the scanning machine
    :param incident_time: the time of the scan incident
    :param threshold_window: amount of time in seconds to search for the process in the connections log (Default is 5s)
    :param threshold_unit: the unit used to calculate the window. Can be "second", "minute", or "hour".
                           Default is seconds
    :return: List of possible processes
    """

    if "T" not in incident_time or incident_time.rfind(":") != (len(incident_time) - 3):
        incident_time = convert_incident_time(incident_time)

    time_window = window_time_range(incident_time, threshold_window, threshold_unit)

    if time_window is None:
        logging.error("Error in retrieving the time window range")
        return None

    [min_time, max_time] = time_window

    searchq = Search(using=client) \
                  .filter("match", _type="barebones_connections") \
                  .filter("match", source_ip=src_ip) \
                  .filter("range", slot_start_time={"gte": min_time, "lt": max_time})[:100000]
    searchq.aggs.bucket('per_src_process', 'terms', field='source_process', size=100000)

    return searchq.execute()


def get_processes_by_incident_time_list(inc_time_list, threshold_window=5, threshold_unit="second"):
    """
    Attempts to find the source processes responsible for the given incident times in inc_time_list.
    Usage --> "inc_proc_total = get_processes_by_incident_time_list(inc_time)"

    :param inc_time_list: List of incident times to look for
    :param threshold_window: amount of time in seconds to search for the process in the connections log (Default is 5s)
    :param threshold_unit: the unit used to calculate the window. Can be "second", "minute", or "hour".
                           Default is seconds
    :return: List of possible processes of each of the given incident times in inc_time_list
    """

    inc_proc_total = [[elem[0], []] for elem in inc_time_list]

    for index in range(len(inc_time_list)):
        inc = inc_time_list[index]
        for inc_time in inc[1]:
            proc_res = get_processes_by_scan_incident_time(inc_proc_total[index][0],
                                                           inc_time[0].strftime("%Y-%m-%dT%H:%M:%S"),
                                                           threshold_window, threshold_unit)
            inc_proc_total[index][1].append(proc_res)

    return inc_proc_total


def get_processes_by_scan_obj(scan_obj, threshold_window=5, threshold_unit="second"):
    """
    Retrieves the processes responsible for initializing the network scan incidents of the given scan object.

    :param scan_obj: JSON format of scan object from mongo
    :param threshold_window: amount of time in seconds to search for the process in the connections log (Default is 5s)
    :param threshold_unit: the unit used to calculate the window. Can be "second", "minute", or "hour".
                           Default is seconds
    :return: List of possible processes
    """

    res = get_processes_by_scan_incident_time(scan_obj["IP"], scan_obj["last_seen"], threshold_window, threshold_unit)
    return res
    # return set([elem["key"] for elem in res.aggregations.per_src_process.buckets._l_])


def check_ports_scan_obj(scan_obj, threshold_num=1000, threshold_range=1023):
    """
    Checks the port activity of the given scan incident by applying simple checks based on the given thresholds.

    TCP Ports range
    - Well-known ports range from 0 through 1023.
    - Registered ports are 1024 to 49151.
    - Dynamic ports (also called private ports) are 49152 to 65535.

    :param scan_obj: Scan incident JSON object as returned from MongoDB
    :param threshold_num: The maximum number of scanned ports considered valid
    :param threshold_range: The highest scanned port number considered valid
    :return: True if the scan incident passes validation, otherwise False
    """

    validated_ports = [port_num for port_num in range(1, threshold_range)]
    non_validated_ports = [port_num for port_num in range((threshold_range + 1), 65535)]

    scanned_ports = set(scan_obj["Ports"])

    # Criteria: Number of scanned ports
    if len(scanned_ports) > threshold_num:
        return False

    # Criteria: Ports out of valid range were scanned
    else:
        for scan_port in scanned_ports:
            if scan_port not in validated_ports:
                return False

    return True


def check_ports_scan_objs(scan_objs, threshold_num=1000, threshold_range=1023):
    """
    Checks the port activity of the given scan incidents by applying simple checks based on the thresholds in
    check_ports_scan_obj().

    :param scan_objs: List of scan incidents (JSON objects) as returned from MongoDB
    :param threshold_num: The maximum number of scanned ports considered valid
    :param threshold_range: The highest scanned port number considered valid
    :return: List of tuples (ip, Boolean) were the boolean value is determined by the criteria in check_ports_scan_obj()
    """

    port_activity_res = []

    for obj in scan_objs:
        port_activity_res.append((obj["IP"], check_ports_scan_obj(obj, threshold_num, threshold_range)))

    return port_activity_res


def get_process_info(process_id, source_ip):
    """
    Retrieves the process info with the given id for the given source ip.

    :param process_id: The id of the process to check for
    :param source_ip: source IP responsible for the scan incident
    :return: Response object of the process with the given id
    """

    searchq = Search(
        using=client).filter("match", _type="processes").filter("match", _id=process_id)\
        .filter("match", ip_address=source_ip)[:100000]

    return searchq.execute()


def get_n_incidents_by_ip(ip_addr, scan_objs, n=10, by_ip=False):
    """
    Retrieves the ids of the min(n, len(incident_obj["incident_ids"])) of the incident object with the
    given IP address.
    scan_objs is a list of dicts of scan objects as queried from mongo.

    :param ip_addr: The IP address of the scan object to look for. pass None if by_ip is False.
    :param scan_objs: List containing scan objects as retrieved from mongo.
    :param n: number of incident ids to return for the scan object. Default is 10.
    :param by_ip: When True, will return the incident ids only for the objects with the given IP.
                  When False, will return the incident ids for all the objects in the given list.
    :return: Incident IDs of min(n, len(scan_obj["incident_ids"]))
    """

    ip_inc = []

    for elem in scan_objs:
        if by_ip:
            if elem["IP"] == ip_addr:
                return tuple((elem["IP"], elem["incident_ids"][:min(n, len(elem["incident_ids"]))]))
        else:
            ip_inc.append((elem["IP"], elem["incident_ids"][:min(n, len(elem["incident_ids"]))]))

    return ip_inc


def get_n_incidents_by_ip_list(ip_list, scan_objs, n=10):
    """
    Retrieves the ids of the min(n, len(incident_obj["incident_ids"])) of the incident object which
    have IP addresses from the given list.
    scan_objs is a list of dicts of scan objects as queried from mongo.

    :param ip_list: List of IP addresses to search by
    :param scan_objs: List containing scan objects as retrieved from mongo.
    :param n: number of incident ids to return for the scan object. Default is 10.
    :return: List of tuples (IP, incident_ids_list) for each IP in the given list
    """

    ip_inc = []

    for ip in ip_list:
        ip_inc.append(get_n_incidents_by_ip(ip, scan_objs, n, True))

    return ip_inc


def get_inc_proc_details_by_inc_proc_res(inc_proc_total):
    """
    Returns processes + their ids and counts from list of ["IP", [ProcessResponseObjs]] instances

    :param inc_proc_total: List of [IP, [process_inc_time_resObj]] instances of incident ips
    :return: List of dictionaries of {"proc_name": count, ..., "ids": [proc_id1, ...]} for each processObj
    in the given list.
    """

    inc_proc_set = []

    for elem in inc_proc_total:
        procs = elem[1]
        all_procs = {}
        for proc in procs:
            if proc.aggregations.per_src_process.buckets._l_:
                for inner_proc in proc.aggregations.per_src_process.buckets._l_:
                    all_procs[inner_proc["key"]] = inner_proc["doc_count"]

        if all_procs:
            all_procs["ids"] = []

        for proc_instance in procs:
            for proc_hit in proc_instance.hits._l_:
                all_procs["ids"].append(proc_hit.source_process_id)

        inc_proc_set.append(all_procs)

    return inc_proc_set


def proc_info_from_proc_set(inc_proc_set, inc_proc_total):
    """
    Get process info response objects from process ids dict of form {"proc_name": count, ..., "ids": [proc_id1, ...]}

    :param inc_proc_set: List of dictionaries of {"proc_name": count, ..., "ids": [proc_id1, ...]}
    :param inc_proc_total: List of possible processes of each if the given incident times in inc_time_list
    :return: List of lists containing process info objects from the "processes" collection type in Elasticsearch
    """

    full_proc_info = []

    for ind in range(len(inc_proc_set)):
        if inc_proc_set[ind]:
            ids = inc_proc_set[ind]["ids"]
            res = []
            for id in ids:
                res.append(get_process_info(id, inc_proc_total[ind][0]))
        else:
            res = []
        full_proc_info.append(res)

    return full_proc_info


def proc_details_from_proc_info(full_proc_info):
    """
    Gets process path and hash from process response objects list as received above "full_proc_info"

    :param full_proc_info: List of lists containing process info objects from the "processes" collection
    type in Elasticsearch as returned by proc_info_from_proc_set()
    :return: List of set of tuples containing (process_path, process_hash) of the processes info objects
    """

    full_proc_details = []
    for procs_coll in full_proc_info:
        inner_proc_info = set()
        for proc_res in procs_coll:
            if hasattr(proc_res.hits._l_[0], "full_path"):
                inner_proc_info.add((proc_res.hits._l_[0].full_path, proc_res.hits._l_[0].image_hash))
        full_proc_details.append(inner_proc_info)

    return full_proc_details


def ports_and_num_of_incs_from_scan_objects_list(scan_records_list):
    """
    Returns 3 lists as follows:
    scanner_ports - Ports scanned by the object
    num_of_incs_per_asset - Number of scan incidents related to the object
    scanner_names - VM name of the object

    :param scan_records_list: List of dictionaries of scanning objects as they appear in the JSON file extracted
    from Mongo

    :return: scanner_ports, num_of_incs_per_asset, scanner_names
    """

    scanner_ports = []
    num_of_incs_per_asset = []
    scanner_names = []

    for elem in scan_records_list:
        scanner_ports.append(set(elem["Ports"]))
        num_of_incs_per_asset.append(elem["incident_ids"])
        scanner_names.append(elem["vm_name"][0])

    return [scanner_ports, num_of_incs_per_asset, scanner_names]


def csv_from_scan_lists(inc_proc_total, inc_proc_set_no_ids, full_proc_details, scanner_ports,
                        scan_agent, num_of_incs_per_asset):
    """
    Outputs scan_details.csv file from the set of lists retrieved from the
    JSON scanner objects file by the previous functions

    :param inc_proc_total: List of possible processes of each of the given incident times in inc_time_list.
    :param inc_proc_set_no_ids: Similar to inc_proc_set but with "ids" = [].
    :param full_proc_details: List of set of tuples (process_path, process_hash) of the processes info objects.
    :param scanner_ports: List of sets of scanned ports of each scanning object.
    :param scan_agent: List of Boolean values stating whether an Agent is installed on the scanning asset.
    :param num_of_incs_per_asset: List of number of incidents related to each asset as it appears in the JSON file.
    :return: None
    """

    if not scan_agent:
        scan_agent = [True] * len(inc_proc_total)

    with open("scan_details.csv", "w") as f:
        csv_headers = ("Scanner IP,Agent installed,Num of incidents," +
                       "Possible processes of incidents,Processes details,Ports\n")
        f.write(csv_headers)
        for index in range(len(inc_proc_total)):
            full_proc = '"' + str(inc_proc_set_no_ids[index]) + '"'
            proc_details = '"' + str(full_proc_details[index]) + '"'
            scanned_ports = '"' + str(scanner_ports[index]) + '"'
            temp_str = ",".join([str(inc_proc_total[index][0]), str(scan_agent[index]),
                                str(num_of_incs_per_asset[index]), full_proc, proc_details,
                                scanned_ports])
            f.write(temp_str + "\n")


# loop to get process info for a list of scan_object processes as returned by get_processes_by_scan_obj() #
###########################################################################################################
# for elem in all_processes: # all_processes is a list of tuples (src_ip, Response_object for scan incident process)
#     if elem[1].hits.total != 0:
#         procs = []
#         for proc_elem in elem[1].hits._l_:
#             procs.append((proc_elem.source_process, get_process_info(proc_elem.source_process_id, elem[0])))
#         proc_info_res.append((elem[0], procs))


# get processes names from list ["IP", [ProcessResponseObjs]] #
###############################################################
# inc_proc_set = []
# for elem in inc_proc_total:
#     procs = elem[1]
#     all_procs = set()
#     for proc in procs:
#         if proc.aggregations.per_src_process.buckets._l_:
#             for inner_proc in proc.aggregations.per_src_process.buckets._l_:
#                 all_procs.add(inner_proc)
#     inc_proc_set.append(all_procs)

# get processes + their ids and counts from list of ["IP", [ProcessResponseObjs]] #
###################################################################################
# for elem in inc_proc_total:
#     procs = elem[1]
#     all_procs = {}
#     for proc in procs:
#         if proc.aggregations.per_src_process.buckets._l_:
#             for inner_proc in proc.aggregations.per_src_process.buckets._l_:
#                 all_procs[inner_proc["key"]] = inner_proc["doc_count"]
#     if all_procs:
#         all_procs["ids"] = []
#     for proc_instance in procs:
#         for proc_hit in proc_instance.hits._l_:
#             all_procs["ids"].append(proc_hit.source_process_id)
#     inc_proc_set.append(all_procs)

# output csv file from inc_proc_total #
#######################################
# with open("scan_details.csv", "w") as f:
#     csv_headers = ("Scanner IP,Agent installed,Num of incidents," +
#                   "Possible processes of first 10 incidents,Processes details,Ports\n")
#     f.write(csv_headers)
#     for index in range(len(inc_proc_total)):
#         temp_proc = str(','.join(inc_proc_set[index]))
#         full_proc = '"' + temp_proc + '"'  # delete this line if using the one below
#         full_proc = '"' + str(inc_proc_set_no_ids[index]) + '"'
#         proc_details = '"' + str(full_proc_details[index]) + '"'
#         scanned_ports = '"' + str(scanner_ports[index]) + '"'
#         temp_str = ",".join([str(inc_proc_total[index][0]), str(scan_agent[index]),
#                             str(num_of_incs_per_asset[index]), full_proc, proc_details,
#                             scanned_ports])
#         f.write(temp_str + "\n")


# Get process info response objects from process ids dict  #
# of form {"proc_name": count, ..., "ids": [proc_id1, ...] #
############################################################

# for ind in range(len(inc_proc_set)):
#     if inc_proc_set[ind]:
#         ids = inc_proc_set[ind]["ids"]
#         res = []
#         for id in ids:
#             res.append(get_process_info(id, inc_proc_total[ind][0]))
#     else:
#         res = []
#     full_proc_info.append(res)

# get process path and hash from process response objects list as received above "full_proc_info" #
###################################################################################################

# full_proc_details = []
# for procs_coll in full_proc_info:
#     inner_proc_info = set()
#     for proc_res in procs_coll:
#         if hasattr(proc_res.hits._l_[0], "full_path"):
#             inner_proc_info.add((proc_res.hits._l_[0].full_path, proc_res.hits._l_[0].image_hash))
#     full_proc_details.append(inner_proc_info)


