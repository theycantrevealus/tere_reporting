# import csv
# import bson
# import pytz
# import re
# import time
# import sys
# import psutil
# import os
import subprocess
import multiprocessing
import numpy as np
import itertools

import pandas as pd
import urllib.parse
from typing import Dict, Iterable
from pymongo import MongoClient
from datetime import *
from dateutil import parser
import configparser

# Function
# =========================================================================================================================================================================
def convert_datetime(dt_str: str):
    return parser.isoparse(dt_str).astimezone()

def formatted_trx_date(dt_str):
    return datetime.strptime(f'{dt_str}'.split("+")[0], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')

def allowed_msisdn(msisdn):
    prefixes = ("08", "62", "81", "82", "83", "85", "628")
    return any(msisdn.startswith(prefix) and msisdn[len(prefix):].isdigit() for prefix in prefixes)

def allowed_indihome_number(msisdn):
    return allowed_msisdn(msisdn) is False

def format_msisdn_to_id(msisdn: str) -> str:
    if msisdn:
        msisdn_str = f'{msisdn}'
        return msisdn_str.replace('08', '628', 1).replace('8', '628', 1) if msisdn_str.startswith(('08', '8')) else msisdn_str
    return msisdn

def format_indihome_number_to_non_core(cust_number):
    return '1' + cust_number[2:] if cust_number and cust_number.startswith('01') else cust_number

def msisdn_combine_format_to_id(msisdn) -> str:
    if allowed_msisdn(msisdn):
        return format_msisdn_to_id(msisdn)
    elif allowed_indihome_number(msisdn):
        return format_indihome_number_to_non_core(msisdn)
    else:
        return ""

def validation_keyword_point_value_rule(payload, total_point=None) -> str:
    if isinstance(payload, dict):
        eligibility = payload.get('keyword', {}).get('eligibility')
        result = 0

        if eligibility:
            if total_point is not None:
                result = total_point
            elif 'total_redeem' in payload.get('incoming', {}):
                result = payload['incoming']['total_redeem']

            point_value = eligibility.get('point_value')  # Corrected key
            if point_value == 'Fixed':
                result = eligibility['point_redeemed']
            elif point_value == 'Flexible':
                if result <= 0:
                    result = eligibility['point_redeemed']
            elif point_value == 'Fixed Multiple':
                if result > 0:
                    result = eligibility['point_redeemed']
        return result
    else:
        return ""

def batch_read(
        collection,
        query,
        batch_size: int = 100
) -> Iterable[pd.DataFrame]:
    cursor = collection.aggregate(query)
    while True:
        batch = list(itertools.islice(cursor, batch_size))
        if not batch:
            break
        yield pd.DataFrame(batch)

# =========================================================================================================================================================================


print("=== 0POIN MANUAL GENERATOR ===")
print("")

# Variable
# =========================================================================================================================================================================
print("From date : ", end = "")
# For latest python
# parse_from = datetime.fromisoformat(input())
parse_from = parser.isoparse(str(input()))
print("")

print("To date : ", end = "")
# For latest python
# parse_from = datetime.fromisoformat(input())
parse_to = parser.isoparse(str(input()))
print("")

# Example : "2024-10-14T17:00:00.000Z 2024-10-15T16:59:00.000Z"


print("File name: ", end = "")
filename = input()
print("")


print("GENERATING...")
print("")
print("")


config = configparser.ConfigParser()
config.read('.env')

ENVIRONMENT = config['ENVIRONMENT']['TARGET']
if ENVIRONMENT == 'development':
    MONGO_URI = "mongodb://" + config['MONGO']['HOST'] + "/"
else:
    MONGO_URI = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']

TARGET_DIR = config['RESULT']['DIR']

BATCH_SIZE_PROCESS = int(config['CONFIG']['BATCH_SIZE'])

filename = TARGET_DIR + "/0POIN/" + filename

process_start_time = datetime.now()
client = MongoClient(MONGO_URI)

pipeline = [
    {
        "$match": {
            "keyword": { "$in": [ "0POIN" ] },
            "transaction_date": {
                "$gte": parse_from,
                "$lte": parse_to
            }
        }
    },
    {
        "$group": {
            "_id": {
                "keyword": "$keyword",
                "msisdn": "$msisdn",
                "isindihome": {
                    "$cond": {
                        "if": {
                            "$regexMatch": {
                                "input": "$msisdn",
                                "regex": "^(08|62|81|82|83|85|628)+[0-9]+$"
                            }
                        },
                        "then": "true",
                        "else": "false"
                    }
                }
            }
        }
    }
]
projection = {
    "_id": 0,
    "msisdn": 1,
    "keyword": 1
}
# =========================================================================================================================================================================

try:
    database = client.get_database(config['MONGO']['DATABASE'])
    collection = database.get_collection("transaction_master")
    with open(filename, "a") as txt_file:
        for batch in batch_read(collection, pipeline, BATCH_SIZE_PROCESS):
            fields = batch.columns.tolist()
            batch_numpy = batch.to_numpy()
            for line in batch_numpy:
                to_write = (
                    f"{line[0].get('msisdn', '')}|"
                    f"{line[0].get('keyword', '')}|"
                    f"{line[0].get('isindihome', '')}"
                )

                txt_file.write(to_write + "\n")
                txt_file.flush()

    client.close()
    # print("===================== MEM. USAGE ========================")
    # process = psutil.Process(os.getpid())
    # mem_usage = process.memory_info().rss / (1024 * 1024)
    # print(f"Memory usage: {mem_usage:.2f} MB")
    print("===================== LINE COUNT ========================")
    subprocess.run(["wc", "-l", filename])
    print("")
    print("")
    print("===================== SAMPLE RESULT =====================")
    subprocess.run(["tail", "-10", filename])
    print("")
    print("")
    print("=========================================================")
    print("--- DONE at [%s] seconds ---" % (datetime.now() - process_start_time))
    print("")
    print("")

except Exception as e:
    raise Exception("Unable to find the document due to the following error: ", e)