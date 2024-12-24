import csv
import bson
import pytz
import re
import time
import sys
import subprocess
import pandas as pd
import urllib.parse

from pymongo import MongoClient
from datetime import *
from dateutil import parser
import configparser

# Function
# =========================================================================================================================================================================
def convert_datetime(dt_str):
    dt = datetime.fromisoformat(dt_str)
    local_dt = dt.astimezone()
    return local_dt

def formatted_trx_date(dt_str):
    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return dt.strftime('%d/%m/%Y %H:%M')

def allowed_msisdn(msisdn):
    prefixes = ["08", "62", "81", "82", "83", "85", "628"]
    for prefix in prefixes:
        if msisdn.startswith(prefix) and msisdn.replace(prefix, "").isdigit():
            return True
    return False

def allowed_indihome_number(msisdn):
    return allowed_msisdn(msisdn) is False

def format_msisdn_to_id(msisdn):
    if msisdn:
        msisdn_str = str(msisdn)
        if msisdn_str.startswith('08') or msisdn_str.startswith('8'):
            return msisdn_str.replace('08', '628', 1).replace('8', '628', 1)
    return msisdn

def format_indihome_number_to_non_core(cust_number):
    if cust_number and cust_number.startswith('01'):
        return '1' + cust_number[2:]
    return cust_number

def msisdn_combine_format_to_id(msisdn):
    if allowed_msisdn(msisdn):
        return format_msisdn_to_id(msisdn)
    elif allowed_indihome_number(msisdn):
        return format_indihome_number_to_non_core(msisdn)
    else:
        return None

def check_null(target, key):
    if key in target.keys():
        return str(target[key])
    else:
        return ""

def iterate_df(df):
    for index, row in df.iterrows():
        yield row.to_dict()

def chunk_process(collection, chunksize=1, start_from=0, query={}, projection={}):
    chunks = range(start_from, collection.count_documents(query), int(chunksize))
    num_chunks = len(chunks)
    for i in range(1,num_chunks+1):
        if i < num_chunks:
            yield collection.find(query, projection=projection)[chunks[i-1]:chunks[i]]
        else:
            yield collection.find(query, projection=projection)[chunks[i-1]:chunks.stop]

# =========================================================================================================================================================================


print("=== FACT DETAIL MANUAL GENERATOR ===")
print("")

# Variable
# =========================================================================================================================================================================
print("From date : ", end = "")
# For latest python
# parse_from = datetime.fromisoformat(input())
# Example : "2024-10-14T17:00:00.000Z"
parse_from = parser.isoparse(str("2024-10-14T17:00:00.000Z"))
print("")

print("To date : ", end = "")
# For latest python
# parse_from = datetime.fromisoformat(input())
# Example : "2024-10-15T16:59:00.000Z"
parse_to = parser.isoparse(str("2024-10-15T16:59:00.000Z"))
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

pipeline = [
    {
        "$match": {
            "transaction_date": {
                "$gte": parse_from,
                "$lte": parse_to
            }
        }
    }
]

query ={
    "transaction_date": {
        "$gte": parse_from,
        "$lte": parse_to
    }
}

output_v = {
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
    "subscriber_brand": 1,
    "program_regionaln": 1,
    "cust_valuen": 1,
    "start_daten": 1,
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
    collection = database.get_collection("fact_detail")
    queryExecute = collection.find(query, output_v, batch_size=10)
    print("======================= CURSOR ==========================")
    print(queryExecute.explain())
    print("")
    print("")
    data = list(queryExecute)
    df = pd.DataFrame.from_records(data)

    mem_usage = sys.getsizeof(df)

    # CURSOR CHUNK
    mess_chunk_iter = chunk_process(collection, BATCH_SIZE_PROCESS, 0, query, output_v)

    chunk_n=0
    total_docs=0
    with open(filename, "a") as txt_file:
        for docs in mess_chunk_iter:
            chunk_n=chunk_n+1
            chunk_len = 0
            for line in docs:
                if not line["transaction_date"] == "":
                    transaction_unformatted = str(convert_datetime(str(str(line["transaction_date"]).split(".")[0].replace(' ', 'T')))).split("+")[0]
                    if not transaction_unformatted == "":
                        transaction_date = str(formatted_trx_date(transaction_unformatted)) or ""
                    else:
                        transaction_date = ""
                else:
                    transaction_date = ""

                if not line["start_date"] == "":
                    start_date_unformatted = str(convert_datetime(str(str(line["start_date"]).split(".")[0].replace(' ', 'T')))).split("+")[0]
                    if not transaction_unformatted == "":
                        start_date = str(formatted_trx_date(start_date_unformatted)) or ""
                    else:
                        start_date = ""
                else:
                    start_date = ""

                if not line["end_date"] == "":
                    end_date_unformatted = str(convert_datetime(str(str(line["end_date"]).split(".")[0].replace(' ', 'T')))).split("+")[0]
                    if not end_date_unformatted == "":
                        end_date = str(formatted_trx_date(end_date_unformatted)) or ""
                    else:
                        end_date = ""
                else:
                    end_date = ""

                msisdn = msisdn_combine_format_to_id(line["msisdn"]) or ""
                keyword = check_null(line, "keyword")
                program_name = check_null(line, "program_name") or ""
                program_owner = check_null(line, "program_owner") or ""
                detail_program_owner = check_null(line, "detail_program_owner") or ""
                created_by = check_null(line, "created_by") or ""
                lifestyle = check_null(line, "liefstyle") or ""
                category = check_null(line, "category") or ""
                keyword_title = check_null(line, "keyword_title") or ""
                SMS = check_null(line, "SMS") or ""
                UMB = check_null(line, "UMB") or ""
                if not line["point"] == "":
                    point = line["point"]
                else:
                    point = "0"
                subscriber_brand = check_null(line, "subscriber_brand") or ""
                program_regional = check_null(line, "program_regional") or ""
                cust_value = check_null(line, "cust_value") or ""
                merchant_name = check_null(line, "merchant_name") or ""
                subscriber_region = check_null(line, "subscriber_region") or ""
                subscriber_branch = check_null(line, "subscriber_branch") or ""
                channel_code = check_null(line, "channel_code") or ""
                subsidy = check_null(line, "subsidy") or ""
                subscriber_tier = check_null(line, "subscriber_tier") or ""
                voucher_code = check_null(line, "voucher_code") or ""
                allowed_IH = str(allowed_indihome_number(line["msisdn"])).lower()
                toWrite = str(transaction_date + "|" +
                              msisdn + "|" +
                              keyword + "|" +
                              program_name + "|" +
                              program_owner + "|" +
                              detail_program_owner + "|" +
                              created_by + "|" +
                              lifestyle + "|" +
                              category + "|" +
                              keyword_title + "|" +
                              SMS + "|" +
                              UMB + "|" +
                              point + "|" +
                              subscriber_brand + "|" +
                              program_regional + "|" +
                              cust_value + "|" +
                              start_date + "|" +
                              end_date + "|" +
                              merchant_name + "|" +
                              subscriber_region + "|" +
                              subscriber_branch + "|" +
                              channel_code + "|" +
                              subsidy + "|" +
                              subscriber_tier + "|" +
                              voucher_code + "|" +
                              allowed_IH + "\n")
                txt_file.write(toWrite)
                txt_file.flush()

                chunk_len=chunk_len+1
                total_docs=total_docs+1

                print(f'chunk #: {chunk_n}, chunk_len: {chunk_len}')
                print("total docs iterated: ", total_docs)

    # with open(filename, "a") as txt_file:
    #     for line in iterate_df(df):
    #         if not line["transaction_date"] == "":
    #             transaction_unformatted = str(convert_datetime(str(str(line["transaction_date"]).split(".")[0].replace(' ', 'T')))).split("+")[0]
    #             if not transaction_unformatted == "":
    #                 transaction_date = str(formatted_trx_date(transaction_unformatted)) or ""
    #             else:
    #                 transaction_date = ""
    #         else:
    #             transaction_date = ""
    #
    #         if not line["start_date"] == "":
    #             start_date_unformatted = str(convert_datetime(str(str(line["start_date"]).split(".")[0].replace(' ', 'T')))).split("+")[0]
    #             if not transaction_unformatted == "":
    #                 start_date = str(formatted_trx_date(start_date_unformatted)) or ""
    #             else:
    #                 start_date = ""
    #         else:
    #             start_date = ""
    #
    #         if not line["end_date"] == "":
    #             end_date_unformatted = str(convert_datetime(str(str(line["end_date"]).split(".")[0].replace(' ', 'T')))).split("+")[0]
    #             if not end_date_unformatted == "":
    #                 end_date = str(formatted_trx_date(end_date_unformatted)) or ""
    #             else:
    #                 end_date = ""
    #         else:
    #             end_date = ""
    #
    #         msisdn = msisdn_combine_format_to_id(line["msisdn"]) or ""
    #         keyword = check_null(line, "keyword")
    #         program_name = check_null(line, "program_name") or ""
    #         program_owner = check_null(line, "program_owner") or ""
    #         detail_program_owner = check_null(line, "detail_program_owner") or ""
    #         created_by = check_null(line, "created_by") or ""
    #         lifestyle = check_null(line, "liefstyle") or ""
    #         category = check_null(line, "category") or ""
    #         keyword_title = check_null(line, "keyword_title") or ""
    #         SMS = check_null(line, "SMS") or ""
    #         UMB = check_null(line, "UMB") or ""
    #         if not line["point"] == "":
    #             point = line["point"]
    #         else:
    #             point = "0"
    #         subscriber_brand = check_null(line, "subscriber_brand") or ""
    #         program_regional = check_null(line, "program_regional") or ""
    #         cust_value = check_null(line, "cust_value") or ""
    #         merchant_name = check_null(line, "merchant_name") or ""
    #         subscriber_region = check_null(line, "subscriber_region") or ""
    #         subscriber_branch = check_null(line, "subscriber_branch") or ""
    #         channel_code = check_null(line, "channel_code") or ""
    #         subsidy = check_null(line, "subsidy") or ""
    #         subscriber_tier = check_null(line, "subscriber_tier") or ""
    #         voucher_code = check_null(line, "voucher_code") or ""
    #         allowed_IH = str(allowed_indihome_number(line["msisdn"])).lower()
    #         toWrite = str(transaction_date + "|" +
    #                       msisdn + "|" +
    #                       keyword + "|" +
    #                       program_name + "|" +
    #                       program_owner + "|" +
    #                       detail_program_owner + "|" +
    #                       created_by + "|" +
    #                       lifestyle + "|" +
    #                       category + "|" +
    #                       keyword_title + "|" +
    #                       SMS + "|" +
    #                       UMB + "|" +
    #                       point + "|" +
    #                       subscriber_brand + "|" +
    #                       program_regional + "|" +
    #                       cust_value + "|" +
    #                       start_date + "|" +
    #                       end_date + "|" +
    #                       merchant_name + "|" +
    #                       subscriber_region + "|" +
    #                       subscriber_branch + "|" +
    #                       channel_code + "|" +
    #                       subsidy + "|" +
    #                       subscriber_tier + "|" +
    #                       voucher_code + "|" +
    #                       allowed_IH + "\n")
    #         txt_file.write(toWrite)
    #         txt_file.flush()
    print(f"Memory usage on cursor data frame: {mem_usage} bytes")
    client.close()
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