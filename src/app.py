import csv
import json
import pandas as pd
import sys

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

NEIGHBOURHOODS_DATA = './data/neighbourhoods_madrid.json'
HOSTS_DATA = './data/hosts_madrid.csv'


def csv_to_json(csv_path):
    df = pd.read_csv(csv_path)
    return df.to_dict(orient='records')


def read_json(json_path):
    with open(json_path, 'r') as f:
        json_file = json.load(f)
    return json_file


def parse_json(original_json):
    return original_json["features"]


def write_element_to_db(element, collection):
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

    db = create_db(client, 'geo_db')  # Create dataBase
    neighbourhoods_collection = create_collection(db, 'neighbourhoods')  # Creates neighbourhoods collection
    hosts_collection = create_collection(db, 'hosts')  # Create hosts collection

    neighbourhoods_data = read_json(neighbourhoods_path)  # Parse neighbourhoods data
    parsed_neighbourhoods_data = parse_json(neighbourhoods_data)

    hosts_collection_data = csv_to_json(HOSTS_DATA)  # Parse hosts data


    # Write to dataBase
    for element in parsed_neighbourhoods_data:
        write_element_to_db(element, neighbourhoods_collection)

    for element in hosts_collection_data:
        write_element_to_db(element, hosts_collection)


if __name__ == '__main__':
    main(NEIGHBOURHOODS_DATA, HOSTS_DATA)
