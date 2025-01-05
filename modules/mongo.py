""" MONGO CONNECTION MANAGER """
import configparser
import urllib.parse
import itertools
from enum import Enum
from typing import Dict, Iterable
from pymongo import MongoClient
import pandas as pd
from modules.file import File

class QueryType(str, Enum):
    """ What should I said ??? """
    FIND = "FIND"
    AGGREGATE = "AGGREGATE"

class Mongo:
    """ What should I said ??? """
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

    def restore(self, path):
        """ What should I said ??? """
        file_instance = File()
        df = file_instance.read_line_json(path)
        self.collection.insert_many(df.to_dict(orient='records'))


    def batch_read(
            self,
            query,
            projection: Dict,
            mode: QueryType = QueryType.FIND,
    ) -> Iterable[pd.DataFrame]:
        """ What should I said ??? """
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