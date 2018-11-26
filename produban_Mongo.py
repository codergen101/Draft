from pymongo import MongoClient, database, collection
from typing import List
import datetime

LABEL_KEY = ["AGENCIAS"]
LABEL_VALUES = ["APLICATIVOS_DE_ATENDIMENTO", "APLICATIVOS_DE_RETAGUARDA", "GESTAO_DOCUMENTEAL_(GDO)",
                "INFRA_SUSTENTACAO_AGENCIAS", "PORTAL_CERTO_(FEC)", "TFC_AGENCIAS_(TFC)", "FRONT_UNICO_(SPA)"]


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

    vm_agent = []
    for name in names:
        if get_record_field_by_field_value(orch_vm_collection, "guest_agent_details", ["name"], [name], [False]):
            vm_agent.append(True)
        else:
            vm_agent.append(False)

    return vm_agent


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
        for s_id in elem[0][1]:
            inc_time = get_record_field_by_field_value(inc_collection, "start_time", ["_id"], [s_id], [False])
            temp_inc_times.append(inc_time)
        inc_times.append((elem[0][0], temp_inc_times))

    return inc_times


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


# Auxiliary helpers #
#####################

# to retrieve label ids by label key and values
# label_ids = get_record_id_by_field_value(label_collection, ["key", "value"], [LABEL_KEY, LABEL_VALUES], [True, True])

# to retrieve vm ids from label ids
# get_record_field_by_field_value(orch_vm_collection, "_id", ["labels"], [label_ids], [True])

