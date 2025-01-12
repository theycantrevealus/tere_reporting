""" What should I said ??? """
import os
import json
import pandas as pd
# Tabulate need dataclass. Dataclass is avail on latest python version
# from tabulate import tabulate
from modules.logger import Logger

class File:
    """ What should I said ??? """
    def __init__(self):
        self.__log = Logger({
            'info': 'info.log',
            'error': 'error.log',
            'warning': 'warning.log',
            'debug': 'debug.log',
            'exception': 'exception.log',
        })

    def backup(self, path, zip_name):
        """ What should I said ??? """
        os.system(f'zip -r {zip_name} {path}')

    def read_line_json(self, path: str):
        """ What should I said ??? """
        data = []
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                data.append(json.loads(line))

        df = pd.DataFrame(data)

        # Exclude field
        df = df.drop('_id', axis=1)

        payload = pd.json_normalize(df['payload'])
        return pd.concat([df, payload], axis=1)

    def mongo_json_to_csv(self, path_from, path_to, projection):
        """ What should I said ??? """
        self.__log.info('Reading JSON')
        df = self.read_line_json(path_from)

        column = df.columns.tolist()
        column_split = 8
        df_column = pd.DataFrame([column[i:i+column_split] for i in range(0, len(column), column_split)])

        print('Available columns:')
        print(df_column)

        df[projection].to_csv(path_to, index=False)
        self.__log.info('Successfully converted JSON to CSV')
        self.csv_viewer(path_to)

    def csv_viewer(self, path):
        """ What should I said ??? """
        df = pd.read_csv(path)
        # print(tabulate(df, headers='keys', tablefmt='psql'))
