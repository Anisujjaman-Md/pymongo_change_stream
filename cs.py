import os
from pymongo import MongoClient
from bson.json_util import dumps

if __name__ == '__main__':
    client = MongoClient(os.environ['CHANGE_STREAM_DB'])

    change_stream = client.tkdc.client_data.watch(full_document='updateLookup')
    documement = next(change_stream)
    for change in change_stream:
        client.tkdc.client_data_history.insert_one(change)
        print(dumps(change))
        print('')