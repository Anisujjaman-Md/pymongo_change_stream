import os
import pymongo
from bson.json_util import dumps
import redis
import json


def get_config():
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    with open(config_file) as f:
        c = json.load(f)

        return c


config = get_config()
redis_host = config["redis_host"]
redis_port = config["redis_port"]


def mongo_trigger():
    source = pymongo.MongoClient(config["source"])
    client_db = source[config["source_db"]]

    destination = pymongo.MongoClient(config["destination"])
    destination_db = destination[config["destination_db"]]

    r = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
    change_stream = client_db.watch(full_document='updateLookup',
                                    resume_after=r.hgetall("resume_token"))
    for change in change_stream:
        r.hset("resume_token", mapping=change_stream.resume_token)
        history_collection_name = str(change.get("ns").get("coll")) + "_history"
        destination_db[history_collection_name].insert_one(change)
        print(dumps(change))


if __name__ == '__main__':
    mongo_trigger()
