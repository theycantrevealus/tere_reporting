import csv
import bson
import pytz
import re
import time
import subprocess

from pymongo import MongoClient
from datetime import *
import configparser


print("=== FACT DETAIL MANUAL GENERATOR ===")
print("")
print("From date : ", end = "")
# parse_from = datetime.fromisoformat(input())
parse_from=datetime.fromisoformat("2024-10-14T17:00:00.000Z")
print("")
print("To date : ", end = "")
# parse_to = datetime.fromisoformat(input())
parse_to=datetime.fromisoformat("2024-10-15T16:59:00.000Z")
print("")
print("File name: ", end = "")
filename = input()

print("")
print("GENERATING...")
print("")
print("")

config = configparser.ConfigParser()
config.read('.env')

MONGO_URI = config['MONGO']['URI']
TARGET_DIR = config['RESULT']['DIR']

filename = TARGET_DIR + "/" + filename

process_start_time = datetime.now()
client = MongoClient(MONGO_URI)

# Function
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
        return target[key]
    else:
        return ""

try:
    database = client.get_database("SLRevamp2")
    movies = database.get_collection("fact_detail")

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
    movie = movies.find(query)
    csv_columns = ['_id','transaction_date','msisdn', 'keyword', 'program_name', 'keyword_title', 'SMS', 'UMB', 'point', 'cust_value', 'start_date','end_date','subscriber_region','subscriber_branch','channel_code','subsidy','subscriber_tier','voucher_code','program_owner','detail_program_owner','created_by','lifestyle','category','program_regional','merchant_name']    
    with open(filename, "w") as txt_file:
        for line in movie:
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


            txt_file.write(transaction_date + "|" +
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
                           allowed_IH +
                           "\n")

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