import os
import subprocess
from datetime import *
from modules.logging import LoggingType, Logging
from modules.mongo import QueryType, Mongo

class ReportTemplate:
    def __init__(self, name):

        self.name = name

        self.Logging = Logging()

        self.Logging.log(LoggingType.INFO, f'Initializing {name}')

        # Create connection
        try:
            self.mongo = Mongo('SLRevamp2', 'transaction_master')
        except Exception as e:
            raise Exception(f'Mongo error {e}')

    def generate(self, from_date, to_date, filename, directory):
        process_start_time = datetime.now()

        self.Logging.log(LoggingType.INFO, f'Generating report')

        target_file_name = f'{directory}/{filename}.csv'

        pipeline = [
            {
                "$match": {
                    "keyword": { "$in": [ "0POIN" ] },
                    "transaction_date": {
                        "$gte": from_date,
                        "$lt": to_date,
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "keyword": "$keyword",
                        "msisdn": "$msisdn"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "keyword": "$_id.keyword",
                    "msisdn": "$_id.msisdn",
                    "isindihome": {
                        "$cond": {
                            "if": {
                                "$regexMatch": {
                                    "input": "$_id.msisdn",
                                    "regex": "^(08|62|81|82|83|85|628)+[0-9]+$"
                                }
                            },
                            "then": "false",
                            "else": "true"
                        }
                    }
                }
            }
        ]

        self.Logging.log(LoggingType.INFO, f'Writing report {target_file_name}')

        with open(target_file_name, "a") as txt_file:
            txt_file.write("MSISDN|KEYWORD|ISINDIHOMENUMBER\n")
            for batch in self.mongo.batch_read(pipeline, {}, QueryType.AGGREGATE):
                fields = batch.columns.tolist()
                batch_numpy = batch.to_numpy()
                for line in batch_numpy:
                    to_write = (
                        f'{line[fields.index("msisdn")]}|'
                        f'{line[fields.index("keyword")]}|'
                        f'{line[fields.index("isindihome")]}'
                    )

                    txt_file.write(to_write + "\n")
                    txt_file.flush()

        self.mongo.client.close()

        self.Logging.log(LoggingType.SUCCESS, f'Report write finished')
        self.Logging.log(LoggingType.INFO, f'Generating control file')
        extension = filename.split('.')[-1]
        with open(target_file_name, "rb") as f:
            row_count = sum(1 for _ in f)

            file_size = os.path.getsize(target_file_name)
            ctl_name = target_file_name.replace(extension, ".ctl")
            with open(ctl_name, "w") as ctl_file:
                ctl_file.write(f'{filename}|{row_count}|{file_size}')

        self.Logging.log(LoggingType.SUCCESS, f'Control file write finished')

        self.Logging.log(LoggingType.INFO, f'LINE COUNT :')
        subprocess.run(["wc", "-l", target_file_name])

        self.Logging.log(LoggingType.INFO, f'Sample Result :')
        subprocess.run(["head", target_file_name])
        print("...")
        print("...")
        print("...")
        subprocess.run(["tail", "-10", target_file_name])
        self.Logging.log(LoggingType.INFO, f'Control file result :')
        subprocess.run(["cat", ctl_name])
        self.Logging.log(LoggingType.SUCCESS, 'DONE at [%s]' % (datetime.now() - process_start_time))

