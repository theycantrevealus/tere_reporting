import configparser
import urllib.parse
import itertools
from enum import Enum
from pymongo import MongoClient
from typing import Dict, Iterable
import pandas as pd

class QueryType(str, Enum):
    FIND = "FIND"
    AGGREGATE = "AGGREGATE"

class Mongo:
    def __init__(self, database, collection, batch_size = 100):
        config = configparser.ConfigParser()
        config.read('.env')
        environment = config['ENVIRONMENT']['TARGET']
        if environment == 'development':
            self.connection_string = "mongodb://" + config['MONGO']['HOST'] + "/"
        else:
            self.connection_string = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']

        self.client = MongoClient(self.connection_string)

        self.database = self.client.get_database(database)
        self.collection = self.database.get_collection(collection)
        self.batch_size = batch_size

    def batch_read(
            self,
            query,
            projection: Dict,
            mode: QueryType = QueryType.FIND,
    ) -> Iterable[pd.DataFrame]:
        if mode == QueryType.FIND:
            cursor = self.collection.find(query, projection = projection)
        elif mode == QueryType.AGGREGATE:
            cursor = self.collection.aggregate(query)
        else:
            raise ValueError("Invalid mode")

        while True:
            batch = list(itertools.islice(cursor, self.batch_size))
            if not batch:
                break
            yield pd.DataFrame(batch)