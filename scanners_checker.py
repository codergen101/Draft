from pymongo import MongoClient, database, collection
from typing import List
import datetime
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import MultiSearch, Search
from time import time
from time import sleep
import json
from elasticsearch_dsl.response import Response
import logging
from copy import deepcopy

LABEL_KEY = ["AGENCIAS"]
LABEL_VALUES = ["APLICATIVOS_DE_ATENDIMENTO", "APLICATIVOS_DE_RETAGUARDA", "GESTAO_DOCUMENTEAL_(GDO)",
                "INFRA_SUSTENTACAO_AGENCIAS", "PORTAL_CERTO_(FEC)", "TFC_AGENCIAS_(TFC)", "FRONT_UNICO_(SPA)"]

# change port if required
client = connections.create_connection(hosts=['localhost:9256'], timeout=1000000)


def get_scanner_records_list(scanner_objs_json="scanner_records.json"):
    """
    Returns a list of dictionaries of the scanning assets from the JSON file.

    :param scanner_objs_json: File containing JSON objects of the scanners as retrieved
    from Mongo
    :return: List of dictionaries of scanning objects as they appear in the JSON file extracted
    from Mongo
    """

    with open(scanner_objs_json, "r") as scanners_file:
        return json.load(scanners_file)


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
    scanner_ips = []

    for elem in scan_records_list:
        scanner_ports.append(set(elem["Ports"]))
        num_of_incs_per_asset.append(len(elem["incident_ids"]))
        scanner_names.append(elem["vm_name"][0])
        scanner_ips.append(elem["IP"])

    return [scanner_ports, num_of_incs_per_asset, scanner_names, scanner_ips]


def connect__gc_mongo(port: int, passwd: str):
    """
    Connects to the GuardiCore Mongo DB.

    :param port: the local port to connect to
    :param passwd: the authentication password to use.
    :return: The "guardicore" db
    """

    mongo_client = MongoClient("localhost", port)
    mongo_client.admin.authenticate("gc_mgmt", passwd)
    gc_db = mongo_client["guardicore"]

    return gc_db


def get_collection(db: database.Database, collection_name: str):
    """
    Retrieves the collection with the given name from the given db.

    :param db: The main db
    :param collection_name: The name of collection to return from the give db
    :return: db[collection_name]
    """

    return db[collection_name]


def get_record_id_by_field_value(gc_collection: collection.Collection, fields: List, fields_values: List,
                                 is_field_array: List):
    """
    Returns the ids of all the records in the given collection where the field with the given name is one
    of the given field_values.

    :param gc_collection: The collection to be queried
    :param fields: List of field to match by.
    :param fields_values: List of Lists of desired values dor each field.
    :param is_field_array: List of Booleans indicating whether the field is an array or not.
    :return: List of ids in which the field is one of the given values
    """

    query = {}

    for field_num in range(len(fields)):
        if is_field_array[field_num]:
            query[fields[field_num]] = {"$in": fields_values[field_num]}
        else:
            query[fields[field_num]] = fields_values[field_num]

    # query["is_on"] = True # uncomment if querying orchestration_vm

    return gc_collection.distinct("_id", query)


def get_record_field_by_field_value(gc_collection: collection.Collection, res_field: str, fields: List,
                                    fields_values: List, is_field_array: List):
    """
    Returns the given res_field of all the records in the given collection where the field with
    the given name is one of the given field_values.

    :param gc_collection: The collection to be queried
    :param res_field: The desired field to be returned
    :param fields: List of field to match by.
    :param fields_values: List of Lists of desired values dor each field.
    :param is_field_array: List of Booleans indicating whether the field is an array or not.
    :return: List of ids in which the field is one of the given values
    """

    query = {}

    for field_num in range(len(fields)):
        # ToDo: replace the is_field_array parameter with type checking of passed field_values
        if is_field_array[field_num]:
            query[fields[field_num]] = {"$in": fields_values[field_num]}
        else:
            query[fields[field_num]] = fields_values[field_num]

    # query["is_on"] = True # uncomment if querying orchestration_vm

    return gc_collection.distinct(res_field, query)


def check_agent_by_vm_name(orch_vm_collection, names):
    """
    Checks whether an agent is installed on which one of the VMs with the given names.

    :param orch_vm_collection: orchestration_vm collection
    :param names: List of names of VMs to check
    :return: List containing True if the VM has agent installed on it, otherwise False
    """

    scan_agent = []
    for name in names:
        if get_record_field_by_field_value(orch_vm_collection, "guest_agent_details", ["name"], [name], [False]):
            scan_agent.append(True)
        else:
            scan_agent.append(False)

    return scan_agent


def get_vm_by_vm_name_in_range(orch_vm_collection: collection.Collection, names: List, last_seen: datetime.datetime):
    """
    Checks whether an agent is installed on which one of the VMs with the given names.
    example of last_seen = datetime.datetime(2018, 10, 1, 00, 00, 00)

    :param orch_vm_collection: orchestration_vm collection
    :param names: List of names of VMs to check
    :param last_seen: Datetime object representing the earliest time for the checked vm to be last seen
    :return: List of Cursors to records in the orchestration_vm collection that match the given criteria
    """

    vm_agent = []

    for name in names:
        res = orch_vm_collection.find(
            {"name": name,
             "last_seen": {"$gt": last_seen}})
        vm_agent.append(res)

    return vm_agent


def agent_check_from_vm_cursor(vm_agent):
    """
    Returns a list containing True if the VM has agent installed on it, otherwise False

    :param vm_agent: List of Cursors to records in the orchestration_vm collection. This is vm_agent
    as returned by get_vm_by_vm_name_in_range

    :return: List containing True if the VM has agent installed on it, otherwise False
    """

    scan_agent = []

    for elem in vm_agent:
        if "guest_agent_details" in elem.next():
            scan_agent.append(True)
        else:
            scan_agent.append(False)

    return scan_agent


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


def get_inc_time_by_inc_id(inc_collection, ip_inc):
    """
    Returns a list of tuples (IP, [start_times of incidents]) of the given incident IDs and their IPs.

    :param inc_collection: 'guardicore_incident' collection of MongoDB
    :param ip_inc: List of tuples (IP, [incident_ids])
    :return: List of tuples (IP, [start_time of incidents]) of the given IPs and their incident IDs
    """

    inc_times = []

    for elem in ip_inc:
        temp_inc_times = []
        for s_id in elem[1]:
            inc_time = get_record_field_by_field_value(inc_collection, "start_time", ["_id"], [s_id], [False])
            temp_inc_times.append(inc_time)

        inc_times.append((elem[0], temp_inc_times))

    return inc_times


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


def get_inc_proc_set_no_ids(inc_proc_set):
    """
    Returns a list similar to inc_proc_set but with ids=[]

    :param inc_proc_set: List of dictionaries of {"proc_name": count, ..., "ids": [proc_id1, ...]} for each processObj
    in the given list.
    :return: List similar to inc_proc_set but with "ids" = []
    """

    inc_proc_set_no_ids = [deepcopy(elem) for elem in inc_proc_set]

    for elem in inc_proc_set_no_ids:
        elem["ids"] = []

    return inc_proc_set_no_ids


def get_process_info(process_id, source_ip):
    """
    Retrieves the process info with the given id for the given source ip.

    :param process_id: The id of the process to check for
    :param source_ip: source IP responsible for the scan incident
    :return: Response object of the process with the given id
    """

    searchq = Search(
        using=client).filter("match", _type="processes").filter("match", _id=process_id) \
                  .filter("match", ip_address=source_ip)[:100000]

    return searchq.execute()


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


def csv_from_scan_lists(inc_proc_total, inc_proc_set_no_ids, full_proc_details, scanner_ports,
                        scan_agent, num_of_incs_per_asset, csv_file_name="scan_details"):
    """
    Outputs scan_details.csv file from the set of lists retrieved from the
    JSON scanner objects file by the previous functions

    :param inc_proc_total: List of possible processes of each of the given incident times in inc_time_list.
    :param inc_proc_set_no_ids: Similar to inc_proc_set but with "ids" = [].
    :param full_proc_details: List of set of tuples (process_path, process_hash) of the processes info objects.
    :param scanner_ports: List of sets of scanned ports of each scanning object.
    :param scan_agent: List of Boolean values stating whether an Agent is installed on the scanning asset.
    :param num_of_incs_per_asset: List of number of incidents related to each asset as it appears in the JSON file.
    :param csv_file_name: Name of output csv file. Default is "scan_details".
    :return: None
    """

    if not scan_agent:
        scan_agent = [True] * len(inc_proc_total)

    with open((csv_file_name + ".csv"), "w") as f:
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


def complete_scan_activity_check(scan_objects_file, orch_vm_collection, incident_collection,
                                 threshold_window=5, threshold_unit="second", incidents=10,
                                 last_seen_agent=None, csv_file_name="scan_details"):
    """
    Constructs a csv file containing all details on possible processes which initialized the scanning events found
    in the scan_objects_file.
    Usage:
    - Connect to mongo and retrieve collections "orchestration_vm" and "incident"
    - Connect to Elasticsearch and define client --> client = connections.create_connection(...)
    - Call the function while passing the full name with extension of the scan_objects file, the two Mongo collections,
      and any desired optional parameters.

    Optional parameters:
    - threshold_window: the amount of time to consider when looking for possible processes. Default is 5
        Note: The passed value is assumed to be a valid one along with the passed unit. The value is not checked.
    - threshold_unit: the unit of time of the threshold_window. Can be "second", "minute", or "hour".
        Default is "second"
    - incidents: the number of incidents to take for each scanning object. Default is 10
        Note: the taken amount would be min(incidents, len(scan_object_incidents)
    - last_seen_agent: datetime object which defines a lower bound for the time required for an asset to be considered
        with valid agent installation.
    - csv_file_name: the name of the output csv file without extension. Default is "scan_details"

    The csv file is written in the script directory.

    :param scan_objects_file: Full name of the JSON file containing the scanner objects.
    :param orch_vm_collection: orchestration_vm Mongo collection
    :param incident_collection: incident Mongo collection
    :param threshold_window: The amount of time to consider when searching for the possible scanning process.
    Default is 5. @see get_processes_by_incident_time_list()

    :param threshold_unit: The unit of time to use for the threshold_window. can be "second", "minute", or "hour".
    Default is "second". @see get_processes_by_incident_time_list()

    :param incidents: Number of incidents to consider when searching for possible processes.
    The taken amount would be min(incidents, len(scan_object_incidents). Default is 10

    :param last_seen_agent: datetime object containing the time required to consider an asset to
    have a valid agent installed. Default is None. @see above

    :param csv_file_name: The name of the output csv file without extension. Default is "scan_details"

    :return: None. The csv file is written in the script directory.
    """

    print("Reading the JSON file")
    scan_records_list = get_scanner_records_list(scan_objects_file)
    [scanner_ports, num_of_incs_per_asset, scanner_names, scanner_ips] = ports_and_num_of_incs_from_scan_objects_list(
        scan_records_list)

    print("Checking agent info of assets")
    # Getting agent info if last_seen_agent datetime was given, otherwise all assets would default to True
    if last_seen_agent:
        vm_agent = get_vm_by_vm_name_in_range(orch_vm_collection, scanner_names, last_seen_agent)
        scan_agent = agent_check_from_vm_cursor(vm_agent)
    else:
        scan_agent = None

    print("Getting incident times using Mongo... this might take a while")
    ip_inc = get_n_incidents_by_ip_list(scanner_ips, scan_records_list, incidents)
    print(ip_inc)
    inc_times = get_inc_time_by_inc_id(incident_collection, ip_inc)
    print(inc_times)
    print("Getting possible process info by incident times using Elasticsearch... this might take a while")
    inc_proc_total = get_processes_by_incident_time_list(inc_times, threshold_window, threshold_unit)
    inc_proc_set = get_inc_proc_details_by_inc_proc_res(inc_proc_total)
    inc_proc_set_no_ids = get_inc_proc_set_no_ids(inc_proc_set)
    full_proc_info = proc_info_from_proc_set(inc_proc_set, inc_proc_total)
    full_proc_details = proc_details_from_proc_info(full_proc_info)

    print("Writing the csv file '" + csv_file_name + ".csv'")
    csv_from_scan_lists(inc_proc_total, inc_proc_set_no_ids, full_proc_details, scanner_ports, scan_agent,
                        num_of_incs_per_asset, csv_file_name)

    print("Done")

# start with scan_records_list which is a list of dictionaries taken from the JSON file
# call ports_and_num_of_incs_from_scan_objects_list() to get four lists
# scanner_ports, num_of_incs_per_asset, scanner_names, and scanner_ips
# You can check agents on each asset by calling get_vm_by_vm_name_in_range() and passing
# its output to agent_check_from_vm_cursor(), or alternatively by calling
# check_agent_by_vm_name() based on names only.
# However, if passing csv_from_scan_lists() None instead of scan_agent it will be
# replaced with all True list.
#######
# To retrieve the csv file
# 1) Call get_n_incidents_by_ip_list() using the scanner_ips and scanner_records_list
# This would give ip_inc
# 2) Call get_inc_time_by_inc_id() using ip_inc
# This would give inc_times
# 3) Call get_processes_by_incident_time_list() using inc_times
# This would give inc_proc_total
# 4) Call get_inc_proc_details_by_inc_proc_res() using inc_proc_total
# This would give inc_proc_set
# 5) Call get_inc_proc_set_no_ids() using inc_proc_set
# This would give inc_proc_set_no_ids
# 6) Call proc_info_from_proc_set() using inc_proc_set and inc_proc_total
# This would give full_proc_info
# 7) Call proc_details_from_proc_info() using full_proc_info
# This would give full_proc_details
# 8) Call csv_from_scan_lists() using all the required lists collected earlier
# This would write a csv file with the possible scanning processes details
