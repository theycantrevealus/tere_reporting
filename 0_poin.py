# import csv
# import bson
# import pytz
# import re
# import time
# import sys
# import psutil
import os
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
print("Date (YYYY-MM-DD): ", end = "")
parse_date = str(input())
print("")

date_obj = pd.to_datetime(parse_date)
last_day = date_obj - pd.Timedelta(days=1)

# Example : "2024-10-14T17:00:00.000Z 2024-10-15T16:59:00.000Z"

print("Custom file name: (Default : TRX_0POIN_<YYYYMMDD>.csv | If defined : TRX_0POIN_<YYYYMMDD><custom_name>.csv)", end = "")
filename = input()
print("")

from_date = parser.isoparse(f'{last_day.strftime("%Y-%m-%d")}T17:00:00.000Z')
to_date = parser.isoparse(f'{parse_date}T17:00:00.000Z')
print(f"GENERATING...[{from_date}] - [{to_date}]")
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

# If set then it will append to file name, if not then will take the default name
fileNameOnly = f'TRX_0POIN_{parse_date.replace("-","")}{filename}.csv'
targetFilename = f'{TARGET_DIR}/0POIN/TRX_0POIN_{parse_date.replace("-","")}{filename}.csv'

process_start_time = datetime.now()
client = MongoClient(MONGO_URI)

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
projection = {
    "_id": 0,
    "msisdn": 1,
    "keyword": 1
}
# =========================================================================================================================================================================

try:
    database = client.get_database(config['MONGO']['DATABASE'])
    collection = database.get_collection("transaction_master")

    with open(targetFilename, "a") as txt_file:
        txt_file.write("MSISDN|KEYWORD|ISINDIHOMENUMBER\n")
        for batch in batch_read(collection, pipeline, BATCH_SIZE_PROCESS):
            fields = batch.columns.tolist()
            batch_numpy = batch.to_numpy()
            for line in batch_numpy:
                # print(f'{line[fields.index("msisdn")]}')
                to_write = (
                    f'{line[fields.index("msisdn")]}|'
                    f'{line[fields.index("keyword")]}|'
                    f'{line[fields.index("isindihome")]}'
                )

                txt_file.write(to_write + "\n")
                txt_file.flush()

    client.close()

    # Write CTL file
    with open(targetFilename, "rb") as f:
        rowCount = sum(1 for _ in f)

    fileSize = os.path.getsize(targetFilename)
    ctlName = targetFilename.replace(".csv", ".ctl")
    with open(ctlName, "w") as ctl_file:
        ctl_file.write(f'{fileNameOnly}|{rowCount}|{fileSize}')

    # print("===================== MEM. USAGE ========================")
    # process = psutil.Process(os.getpid())
    # mem_usage = process.memory_info().rss / (1024 * 1024)
    # print(f"Memory usage: {mem_usage:.2f} MB")
    print("===================== LINE COUNT ========================")
    subprocess.run(["wc", "-l", targetFilename])
    print("")
    print("")
    print("===================== SAMPLE RESULT =====================\033[93m")
    subprocess.run(["head", targetFilename])
    print("...")
    print("...")
    print("...")
    subprocess.run(["tail", "-10", targetFilename])
    print("")
    print("")
    print("\033[0m===================== CTL RESULT =====================")
    subprocess.run(["cat", ctlName])
    print("")
    print("")
    print("=========================================================")
    print("\033[92m--- DONE at [%s] seconds ---\033[0m" % (datetime.now() - process_start_time))
    print("")
    print("")

except Exception as e:
    raise Exception("Unable to find the document due to the following error: ", e)