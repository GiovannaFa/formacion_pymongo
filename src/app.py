import json
import sys

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

NEIGHBOURHOODS_DATA = './data/neighbourhoods_madrid.json'
HOSTS_DATA = './data/hosts_madrid.csv'


def read_csv(csv_path):
    with open(csv_path, 'r') as f:
        csv_file = json.load(f)
    return csv_file


def read_json(json_path):
    with open(json_path, 'r') as f:
        json_file = json.load(f)
    return json_file


def parse_json(original_json):
    return original_json["features"]


def write_to_db(element, collection):
    collection.insert_one(element)


def create_db(client, db_name):
    return client[db_name]


def create_collection(database, collection_name):
    return database[collection_name]


def main(neighbourhoods_path, hosts_path):
    """ Connect to MongoDB """
    try:
        client = MongoClient(host="localhost", port=27017)
        print("Connected successfully")
    except ConnectionFailure as e:
        sys.stderr.write("Could not connect to MongoDB: {}".format(e))
        sys.exit(1)

    db = create_db(client, 'geo_db')
    collection = create_collection(db, 'geo_collection')

    neighbourhoods_data = read_json(neighbourhoods_path)
    parsed_neighbourhoods_data = parse_json(neighbourhoods_data)

    for element in parsed_neighbourhoods_data:
        write_to_db(element, collection)


if __name__ == '__main__':
    main(NEIGHBOURHOODS_DATA, HOSTS_DATA)
