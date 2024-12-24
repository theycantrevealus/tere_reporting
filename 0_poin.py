import csv
import bson
import pytz
import re
import time
import subprocess
from pymongo import MongoClient
from datetime import *
import configparser


print("=== 0POIN MANUAL GENERATOR ===")
print("")
print("From date : ", end = "")
parse_from = datetime.fromisoformat(input())
# parse_from=datetime.fromisoformat("2024-10-14T17:00:00.000Z")
print("")
print("To date : ", end = "")
parse_to = datetime.fromisoformat(input())
# parse_to=datetime.fromisoformat("2024-10-15T16:59:00.000Z")
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


try:
    database = client.get_database("SLRevamp2")
    transaction_master_detail = database.get_collection("transaction_master")

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

    data_set = transaction_master_detail.aggregate(pipeline)
    with open(filename, "w") as txt_file:
        for line in data_set:
            txt_file.write(line["_id"]["msisdn"] + "|" +
                           line["_id"]["keyword"] + "|" +
                           line["_id"]["isindihome"] +
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