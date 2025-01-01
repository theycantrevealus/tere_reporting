import os
import json
import pandas as pd
from modules.logging import LoggingType

class File:
    def __init__(self):
        from modules.logging import Logging
        self.Logging = Logging()

    def backup(self, path, zip_name):
        os.system(f'zip -r {zip_name} {path}')

    def read_line_json(self, path: str):
        data = []
        with open(path, 'r') as f:
            for line in f:
                data.append(json.loads(line))

        df = pd.DataFrame(data)

        # Exclude field
        df = df.drop('_id', axis=1)

        payload = pd.json_normalize(df['payload'])
        return pd.concat([df, payload], axis=1)

    def mongo_json_to_csv(self, path_from, path_to, projection):
        self.Logging.log(LoggingType.INFO, 'Reading JSON')
        df = self.read_line_json(path_from)

        column = df.columns.tolist()
        column_split = 8
        df_column = pd.DataFrame([column[i:i+column_split] for i in range(0, len(column), column_split)])

        print('Available columns:')
        print(df_column)

        df[projection].to_csv(path_to, index=False)
        self.Logging.log(LoggingType.SUCCESS, 'Successfully converted JSON to CSV')
        self.csv_viewer(path_to)

    def csv_viewer(self, path):
        from tabulate import tabulate
        df = pd.read_csv(path)
        print(tabulate(df, headers='keys', tablefmt='psql'))
