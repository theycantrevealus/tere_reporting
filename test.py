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
        query: Dict = {},
        projection: Dict = {},
        batch_size: int = 100
) -> Iterable[pd.DataFrame]:
    cursor = collection.find(query, projection=projection)
    while True:
        batch = list(itertools.islice(cursor, batch_size))
        if not batch:
            break
        yield pd.DataFrame(batch)

# =========================================================================================================================================================================


print("=== FACT DETAIL MANUAL GENERATOR ===")
print("")

# Variable
# =========================================================================================================================================================================
# For latest python
# parse_from = datetime.fromisoformat(input())
# Example : "2024-10-14T17:00:00.000Z"

print("From date : ", end = "")
parse_from = parser.isoparse(str(input()))
print("")


# For latest python
# parse_from = datetime.fromisoformat(input())
# Example : "2024-10-15T16:59:00.000Z"

print("To date : ", end = "")
parse_to = parser.isoparse(str(input()))
print("")

print("Target Collection: ", end = "")
target_collection = input()
print("")

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

filename = TARGET_DIR + "/" + filename

process_start_time = datetime.now()
client = MongoClient(MONGO_URI)

# pipeline = [
#     {
#         "$match": {
#             "transaction_date": {
#                 "$gte": parse_from,
#                 "$lte": parse_to
#             }
#         }
#     }
# ]

query = {
    "transaction_date": {
        "$gte": parse_from,
        "$lte": parse_to
    }
}

# query = {
#     "_id": {
#         "$exists": True
#     }
# }

projection = {
    "_id": 0,
    "transaction_date": 1,
    "start_date": 1,
    "end_date": 1,
    "msisdn": 1,
    "keyword": 1,
    "program_name": 1,
    "program_owner": 1,
    "detail_program_owner": 1,
    "created_by": 1,
    "lifestyle": 1,
    "category": 1,
    "keyword_title": 1,
    "SMS": 1,
    "UMB": 1,
    "point": 1,
    "subscriber_brand": { "$ifNull": ["$subscriber_brand", "123"] },
    "program_regional": 1,
    "cust_value": 1,
    "merchant_name": 1,
    "subscriber_region": 1,
    "subscriber_branch": 1,
    "channel_code": 1,
    "subsidy": 1,
    "subscriber_tier": 1,
    "voucher_code": 1
}
# =========================================================================================================================================================================

try:
    database = client.get_database(config['MONGO']['DATABASE'])
    collection = database.get_collection(f"{target_collection}")

    # queryExecute = collection.find(query, output_v, batch_size=10)
    # print("======================= CURSOR ==========================")
    # print(queryExecute.explain())
    # print("")
    # print("")

    # data = list(queryExecute)
    # df = pd.DataFrame.from_records(data)
    #
    # mem_usage = sys.getsizeof(df)

    with open(filename, "a") as txt_file:
        for batch in batch_read(collection, query, projection, BATCH_SIZE_PROCESS):
            fields = batch.columns.tolist()
            batch_numpy = batch.to_numpy()
            for line in batch_numpy:
                transaction_date = ""
                if line[fields.index('transaction_date')]:
                    transaction_date_unformatted = convert_datetime(f'{line[fields.index("transaction_date")]}'.replace(' ', 'T').split('.')[0])
                    transaction_date = f'{formatted_trx_date(transaction_date_unformatted)}' or ""

                start_date = ""
                if line[fields.index('start_date')]:
                    start_date_unformatted = convert_datetime(f'{line[fields.index("start_date")]}'.replace(' ', 'T').split('.')[0])
                    start_date = f'{formatted_trx_date(start_date_unformatted)}' or ""

                end_date = ""
                if line[fields.index('end_date')]:
                    end_date_unformatted = convert_datetime(f'{line[fields.index("end_date")]}'.replace(' ', 'T').split('.')[0])
                    end_date = f'{formatted_trx_date(end_date_unformatted)}' or ""

                allowed_IH = f'{allowed_indihome_number(line[fields.index("msisdn")])}'.lower()

                to_write = (
                    f"{transaction_date}|"
                    f"{line[fields.index('msisdn')]}|"
                    f"{line[fields.index('keyword')]}|"
                    f"{line[fields.index('program_name')]}|"
                    f"{line[fields.index('program_owner')]}|"
                    f"{line[fields.index('detail_program_owner')]}|"
                    f"{line[fields.index('created_by')]}|"
                    f"{line[fields.index('lifestyle')]}|"
                    f"{line[fields.index('category')]}|"
                    f"{line[fields.index('keyword_title')]}|"
                    f"{line[fields.index('SMS')]}|"
                    f"{line[fields.index('UMB')]}|"
                    f"{validation_keyword_point_value_rule(line[fields.index('point')]) or ''}|"
                    f"{line[fields.index('subscriber_brand') or '']}|"
                    f"{line[fields.index('program_regional')]}|"
                    f"{line[fields.index('cust_value')]}|"
                    f"{start_date}|"
                    f"{end_date}|"
                    f"{line[fields.index('merchant_name')]}|"
                    f"{line[fields.index('subscriber_region')]}|"
                    f"{line[fields.index('subscriber_branch')]}|"
                    f"{line[fields.index('channel_code')]}|"
                    f"{line[fields.index('subsidy')]}|"
                    f"{line[fields.index('subscriber_tier')]}|"
                    f"{line[fields.index('voucher_code')]}|"
                    f"{allowed_IH}"
                )

                txt_file.write(to_write + "\n")
                txt_file.flush()

    client.close()

    # Write CTL file
    with open(filename, "rbU") as f:
        rowCount = sum(1 for _ in f)

    fileSize = os.path.getsize(filename)
    ctlName = filename.replace(".dat", ".ctl")
    with open(ctlName, "w") as ctl_file:
        ctl_file.write(f'{filename}|{rowCount}|{fileSize}')


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