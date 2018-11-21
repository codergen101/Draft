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


def get_vm_by_vm_name_in_range(orch_vm_collection: collection.Collection, names):
    """
    Checks whether an agent is installed on which one of the VMs with the given names.

    :param orch_vm_collection: orchestration_vm collection
    :param names: List of names of VMs to check
    :return: List containing True if the VM has agent installed on it, otherwise False
    """

    # ToDo: implement the time range mechanism

    vm_agent = []

    for name in names:
        res = orch_vm_collection.find(
            {"name": name,
             "last_seen": {"$gt": datetime.datetime(2018, 10, 1, 00, 00, 00)}})
        vm_agent.append(res)

    return vm_agent


# Auxiliary helpers #
#####################

# to retrieve label ids by label key and values
# label_ids = get_record_id_by_field_value(label_collection, ["key", "value"], [LABEL_KEY, LABEL_VALUES], [True, True])

# to retrieve vm ids from label ids
# get_record_field_by_field_value(orch_vm_collection, "_id", ["labels"], [label_ids], [True])

