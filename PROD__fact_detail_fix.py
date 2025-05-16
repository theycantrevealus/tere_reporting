# import csv
# import bson
# import pytz
# import re
# import time
import sys
# import psutil
import os
import json
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
import math

# Function
# =========================================================================================================================================================================
def convert_datetime(dt_str: str):
    return parser.isoparse(dt_str).astimezone()

def formatted_trx_date(dt_str):
    dt_obj = pd.to_datetime(str(dt_str).split("+")[0], format='%Y-%m-%d %H:%M:%S')
    dt_obj += pd.Timedelta(hours=7)
    return dt_obj.strftime('%d/%m/%Y %H:%M')

#def formatted_trx_date(dt_str):
#    return datetime.strptime(f'{dt_str}'.split("+")[0], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')

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
                # print(f'Incoming {result}')

            point_value = eligibility.get('poin_value')  # Corrected key
            if point_value == 'Fixed':
                result = eligibility['poin_redeemed']
                # prinf(f'Fixed {result}')
            elif point_value == 'Flexible':
                if result <= 0:
                    result = eligibility['poin_redeemed']
                    # print(f'Flexible {result}')
            elif point_value == 'Fixed Multiple':
                if result > 0:
                    result = eligibility['poin_redeemed']
                    # print(f'Fixed Multiple {result}')
            # else:
                # print(json.dumps(eligibility, indent=4, sort_keys=True, separators=(',',': ')))
                # print(f'PV {payload.get("keyword").get("eligibility")}')
                # print(f'Point Value {point_value}')
        return result
    else:
        # print(payload)
        return "0"

def batch_read(
        collection,
        query: Dict = {},
        projection: Dict = {},
        batch_size: int = 100
) -> Iterable[pd.DataFrame]:
    cursor = collection.aggregate(query)
    # cursor = collection.find(query, projection=projection)
    while True:
        batch = list(itertools.islice(cursor, batch_size))
        if not batch:
            break
        yield pd.DataFrame(batch)

def check_empty(nilai):
    """What should i do?"""
    if nilai is None or (isinstance(nilai, float) and math.isnan(nilai)) or nilai == "" or (isinstance(nilai, list) and len(nilai) == 0):
        return ""
    else:
        return nilai

# =========================================================================================================================================================================


print("=== FACT DETAIL MANUAL GENERATOR ===")
print("")

# Variable
# =========================================================================================================================================================================
# For latest python
# parse_from = datetime.fromisoformat(input())
# Example : "2024-10-14T17:00:00.000Z"

print("Target date : ", end = "")
# parse_date = str(input())
PARSE_DATE = str(sys.argv[1])
date_obj = pd.to_datetime(PARSE_DATE)
last_day = date_obj - pd.Timedelta(days=1)
parse_from = parser.isoparse(f'{last_day.strftime("%Y-%m-%d")}T17:00:00.000Z')
parse_to = parser.isoparse(f'{PARSE_DATE}T17:00:00.000Z')
print(f"From {parse_from} to {parse_to}")

#parse_from = parser.isoparse(str(input()))
#parse_from = parser.isoparse("2023-12-15T17:00:00.000Z")
#print("")


# For latest python
# parse_from = datetime.fromisoformat(input())
# Example : "2024-10-15T16:59:00.000Z"

#print("To date : ", end = "")
#parse_to = parser.isoparse("2026-12-10T17:00:00.000Z")
#print("")

# print("Target Collection: ", end = "")
target_collection = "transaction_master"
# target_collection = input()
#target_collection = "fact_detail"
print("")

filename = str(sys.argv[2])
print(f"File name: {filename}")
# print(f"File name: {filename}", end = "")
# filename = input()

#filename = "testing_tanaka.dat"
#print("")


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

single_filename = filename
filename = TARGET_DIR + "/" + filename

process_start_time = datetime.now()
client = MongoClient(MONGO_URI)


query = [
    {
        "$match": {
            "transaction_date": {
                "$gte": parse_from,
                "$lt": parse_to,
            },
            "status": 'Success',
            "origin": {
                "$regex": "^redeem"
            }
        }
    },
    {
        "$lookup": {
            "from": "transaction_master_detail",
            "localField": "transaction_id",
            "foreignField": "payload.redeem.master_id",
            "as": "transaction_detail",
        }
    },
    {
        "$project": {
            "_id": 0,
            "transaction_date": "$transaction_date",
            "msisdn": "$msisdn",
            "keyword": "$keyword",
            "program_name": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.program"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload.program.name",
                            0
                        ]
                    }
                }
            },
            "program_owner": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.program"
                            },
                            0
                        ]
                    },
                    "then": '',
                    "else": {
                        "$convert": {
                            "input": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.program.program_owner",
                                    0,
                                ]
                            },
                            "to": 'objectId',
                            "onError": 'null',
                        }
                    }
                }
            },
            "detail_program_owner": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.program"
                            },
                            0
                        ]
                    },
                    "then": '',
                    "else": {
                        "$convert": {
                            "input": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.program.program_owner_detail",
                                    0,
                                ]
                            },
                            "to": "objectId",
                            "onError": "null"
                        }
                    }
                }
            },
            "created_by": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$ifNull": [
                                    "$created_by", "null"
                                ]
                            },
                            "null"
                        ]
                    },
                    "then": '',
                    "else": {
                        "$convert": {
                            "input": "$created_by",
                            "to": "objectId",
                            "onError": "null",
                        }
                    }
                }
            },
            "lifestyle": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.keyword.eligibility",
                            },
                            0
                        ]
                    },
                    "then": '',
                    "else": {
                        "$convert": {
                            "input": {
                                "$arrayElemAt": [
                                    {
                                        "$arrayElemAt": [
                                            "$transaction_detail.payload.keyword.eligibility.program_experience",
                                            0,
                                        ],
                                    },
                                    0
                                ]
                            },
                            "to": "objectId",
                            "onError": "null",
                        }
                    }
                }
            },
            "keyword_title": "$keyword",
            "SMS": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.incoming"
                            },
                            0
                        ]
                    },
                    "then": '0',
                    "else": {
                        "$cond": {
                            "if": {
                                "$regexMatch": {
                                    "input": {
                                        "$toLower": {
                                            "$arrayElemAt": [
                                                "$transaction_detail.payload.incoming.channel_id",
                                                0,
                                            ]
                                        }
                                    },
                                    "regex": "sms",
                                }
                            },
                            "then": "1",
                            "else": "0",
                        }
                    }
                }
            },
            "UMB": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.incoming"
                            },
                            0
                        ]
                    },
                    "then": '0',
                    "else": {
                        "$cond": {
                            "if": {
                                "$regexMatch": {
                                    "input": {
                                        "$toLower": {
                                            "$arrayElemAt": [
                                                "$transaction_detail.payload.incoming.channel_id",
                                                0,
                                            ]
                                        }
                                    },
                                    "regex": "umb",
                                }
                            },
                            "then": "1",
                            "else": "0",
                        }
                    }
                }
            },
            "point": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload",
                            0
                        ]
                    }
                }
            },
            "subscriber_brand": {
                "$arrayElemAt": [
                    "$transaction_detail.payload.customer.brand",
                    0
                ]
            },
            "program_regional": {
                "$substr": [
                    "$msisdn",
                    2,
                    6
                ]
            },
            "cust_value": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.keyword"
                            },
                            0
                        ],
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload.keyword.eligibility.customer_value",
                            0,
                        ]
                    }
                }
            },
            "start_date": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.keyword"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload.keyword.eligibility.start_period",
                            0,
                        ]
                    }
                }
            },
            "end_date": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.keyword"
                            },
                            0
                        ],
                    },
                    "then": '',
                    "else": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload.keyword.eligibility.end_period",
                            0,
                        ]
                    }
                }
            },
            "merchant_name": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.keyword"
                            },
                            0
                        ]
                    },
                    "then": '',
                    "else": {
                        "$convert": {
                            "input": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.keyword.eligibility.merchant",
                                    0,
                                ],
                            },
                            "to": "objectId",
                            "onError": "null",
                        },
                    },
                },
            },
            "subscriber_region": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.customer"
                            },
                            0
                        ],
                    },
                    "then": "OTHERS",
                    "else": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload.customer.region",
                            0,
                        ],
                    },
                },
            },
            "subscriber_branch": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.customer"
                            },
                            0
                        ],
                    },
                    "then": "OTHERS",
                    "else": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload.customer.city",
                            0,
                        ],
                    },
                },
            },
            "channel_code": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$ifNull": [
                                    "$channel_id",
                                    "null"
                                ]
                            },
                            "null"
                        ]
                    },
                    "then": "",
                    "else": "$channel_id",
                },
            },
            "subsidy": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.keyword.eligibility.program_bersubsidi",
                                    0,
                                ],
                            },
                            "true",
                        ],
                    },
                    "then": "Y",
                    "else": "N",
                },
            },
            "subscriber_tier": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {"$size": "$transaction_detail.payload.customer"
                                },
                            0
                        ],
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.customer.loyalty_tier.name",
                                    0,
                                ],
                            },
                            0,
                        ],
                    },
                },
            },
            "voucher_code": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$transaction_detail.payload.payload.voucher.core",
                            },
                            0,
                        ],
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload.payload.voucher.core.voucher_code",
                            0,
                        ],
                    },
                },
            },
        },
    },
    {
        "$lookup": {
            "from": "accounts",
            "localField": "created_by",
            "foreignField": "_id",
            "as": "created_by",
        },
    },
    {
        "$lookup": {
            "from": "lovs",
            "localField": "program_owner",
            "foreignField": "_id",
            "as": "program_owner",
        },
    },
    {
        "$lookup": {
            "from": "locations",
            "localField": "detail_program_owner",
            "foreignField": "_id",
            "as": "detail_program_owner",
        },
    },
    {
        "$lookup": {
            "from": "lovs",
            "localField": "lifestyle",
            "foreignField": "_id",
            "as": "lifestyle",
        },
    },
    {
        "$lookup": {
            "from": "locationprefixes",
            "localField": "program_regional",
            "foreignField": "prefix",
            "as": "program_regional",
        },
    },
    {
        '$lookup': {
            "from": "merchantv2",
            "localField": "merchant_name",
            "foreignField": "_id",
            "as": "merchant_name",
        },
    },
    {
        "$project": {
            "transaction_date": 1,
            "msisdn": 1,
            "keyword": 1,
            "program_name": 1,
            "program_owner": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$program_owner"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$program_owner.set_value",
                            0
                        ]
                    },
                },
            },
            "detail_program_owner": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$detail_program_owner"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$detail_program_owner.name",
                            0
                        ]
                    },
                },
            },
            "created_by": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$created_by"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$created_by.user_name",
                            0
                        ]
                    },
                },
            },
            "lifestyle": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$lifestyle"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$lifestyle.set_value",
                            0
                        ]
                    },
                },
            },
            "category": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$lifestyle"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$lifestyle.set_value",
                            0
                        ]
                    },
                },
            },
            "keyword_title": 1,
            "SMS": 1,
            "UMB": 1,
            "point": 1,
            "subscriber_brand": 1,
            "program_regional": {
                "$toUpper": {
                    "$arrayElemAt": [
                        "$program_regional.area",
                        0
                    ]
                },
            },
            "cust_value": 1,
            "start_date": 1,
            "end_date": 1,
            "merchant_name": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$size": "$merchant_name"
                            },
                            0
                        ]
                    },
                    "then": "",
                    "else": {
                        "$arrayElemAt": [
                            "$merchant_name.merchant_name",
                            0
                        ]
                    },
                }
            },
            "subscriber_region": 1,
            "subscriber_branch": 1,
            "channel_code": 1,
            "subsidy": 1,
            "subscriber_tier": 1,
            "voucher_code": 1
        },
    }
]

#query = {}

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
                    f"{check_empty(line[fields.index('msisdn')])}|"
                    f"{check_empty(line[fields.index('keyword')])}|"
                    f"{check_empty(line[fields.index('program_name')])}|"
                    f"{check_empty(line[fields.index('program_owner')])}|"
                    f"{check_empty(line[fields.index('detail_program_owner')])}|"
                    f"{check_empty(line[fields.index('created_by')])}|"
                    f"{check_empty(line[fields.index('lifestyle')])}|"
                    f"{check_empty(line[fields.index('category')])}|"
                    f"{check_empty(line[fields.index('keyword_title')])}|"
                    f"{check_empty(line[fields.index('SMS')])}|"
                    f"{check_empty(line[fields.index('UMB')])}|"
                    f"{validation_keyword_point_value_rule(line[fields.index('point')])}|"
                    f"{check_empty(line[fields.index('subscriber_brand') or ''])}|"
                    f"{check_empty(line[fields.index('program_regional')])}|"
                    f"{check_empty(line[fields.index('cust_value')])}|"
                    f"{start_date}|"
                    f"{end_date}|"
                    f"{check_empty(line[fields.index('merchant_name')])}|"
                    f"{check_empty(line[fields.index('subscriber_region')])}|"
                    f"{check_empty(line[fields.index('subscriber_branch')])}|"
                    f"{check_empty(line[fields.index('channel_code')])}|"
                    f"{check_empty(line[fields.index('subsidy')])}|"
                    f"{check_empty(line[fields.index('subscriber_tier')])}|"
                    f"{check_empty(line[fields.index('voucher_code')])}|"
                    f"{allowed_IH}"
                )

                txt_file.write(to_write + "\n")
                txt_file.flush()

    client.close()

    # Write CTL file
    with open(filename, "rb") as f:
        rowCount = sum(1 for _ in f)

    fileSize = os.path.getsize(filename)
    ctlName = filename.replace(".dat", ".ctl")
    with open(ctlName, "w") as ctl_file:
        ctl_file.write(f'{single_filename}|{rowCount}|{fileSize}')


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
    print("===================== CONTROL RESULT =====================")
    subprocess.run(["cat", ctlName])
    print("")
    print("")
    print("===================== TOTAL POINT ========================")
    subprocess.run(["awk", "-F", "|", "{totalPoin += $13}END{print totalPoin}", filename])
    print("")
    print("")
    print("=========================================================")
    print("--- DONE at [%s] seconds ---" % (datetime.now() - process_start_time))
    print("")
    print("")

except Exception as e:
    raise Exception("Unable to find the document due to the following error: ", e)
