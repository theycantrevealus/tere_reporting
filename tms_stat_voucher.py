"""
    MERCHANT SUITE STAT::VOUCHHER
    This script retrieves voucher statistics from Tere transaction.
    It requires a configuration file (.env) with API credentials.   
"""
import configparser
import urllib.parse
from typing import Dict
import shutil
import itertools
from datetime import datetime
import pandas as pd
from pymongo import MongoClient

print("=== MERCHANT SUITE STAT::VOUCHHER ===")
print("")
config = configparser.ConfigParser()
config.read('.env')
BATCH_SIZE_PROCESS = int(config['CONFIG']['BATCH_SIZE'])
PAD_LENGTH = 100
TERMINAL_WIDTH = shutil.get_terminal_size().columns
ENVIRONMENT = config['ENVIRONMENT']['TARGET']
START_TIME = datetime.now()
if ENVIRONMENT == 'development':
    MONGO_URI = "mongodb://" + config['MONGO']['HOST'] + "/"
else:
    MONGO_URI = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']
client = MongoClient(MONGO_URI)


# FUNCTIONS
def c_print(first_string, second_string, total_length=80):
    """ STATUS REPORT"""
    first_string_length = len(first_string)
    second_string_length = len(second_string)
    padding_length = total_length - first_string_length - second_string_length
    if padding_length > 0:
        padded_string = first_string + '.' * padding_length + second_string
        print(padded_string)
    else:
        print(first_string + ' ' + second_string)

def fetch(collection, query: Dict, batch_size: int = 100) -> pd.DataFrame:
    """DATA FETCH"""
    cursor = collection.aggregate(query)
    dfs = []
    while True:
        batch = list(itertools.islice(cursor, batch_size))
        if not batch:
            break
        df = pd.DataFrame(batch)
        df = df.fillna(0)
        dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# def fetch(
#         collection,
#         query: Dict,
#         batch_size: int = 100
# ) -> Iterable[pd.DataFrame]:
#     """ DATA FETCH """
#     cursor = collection.aggregate(query)
#     # cursor = collection.find(query, projection=projection)
#     while True:
#         batch = list(itertools.islice(cursor, batch_size))
#         if not batch:
#             break
#         df = pd.DataFrame(batch)
#         df = df.fillna(0)
#         yield df

try:
    db_tms = client.get_database(config['MONGO']['DATABASE_TMS'])
    db_core_voucher = client.get_database(config['MONGO']['DATABASE_CORE_VOUCHER'])
    db_noncore = client.get_database(config['MONGO']['DATABASE_NONCORE'])

    # 1. RETRIEVE ACTIVE MERCHANTS
    c_print("✨ FETCHING ACTIVE MERCHANTS FROM TMS SOURCE", "ms_merchants" , PAD_LENGTH)
    col_merchants = db_tms.get_collection('ms_merchants')
    merchant_a = fetch(col_merchants, [{
        "$match": {
            "merchant_status": "ACTIVE"
        }
    },
    {
        "$group": {
            "_id": "$shortcode",
            "doc": {
                "$first": "$$ROOT"
            }
            }
    },
    {
        "$replaceRoot": {
            "newRoot": "$doc"
        }
    },
    {
        "$project": {
            "_id": 1,
            "shortcode": 1,
        }
    }], BATCH_SIZE_PROCESS)
    c_print("✨ FOUND ACTIVE MERCHANTS", f"[{len(merchant_a)}] (active/unique) merchants found" , PAD_LENGTH)
    print('―' * TERMINAL_WIDTH)

    # 2. MATCH ON NON CORE MERCHANTS
    c_print("✨ FETCHING MERCHANTS FROM NON-CORE SOURCE", "merchantv2" , PAD_LENGTH)
    col_noncore_merchants = db_noncore.get_collection('merchantv2')
    noncore_merchant_a = fetch(col_noncore_merchants, [{
        "$match": {
            "merchant_short_code": {
                "$in": merchant_a['shortcode'].tolist()
            }
        }
    },
    {
        "$group": {
            "_id": "$merchant_short_code",
            "doc": {
                "$first": "$$ROOT"
            }
            }
    },
    {
        "$replaceRoot": {
            "newRoot": "$doc"
        }
    },
    {
        "$project": {
            "_id": 1,
            "merchant_short_code": 1
        }
    }], BATCH_SIZE_PROCESS)
    c_print("✨ FOUND MERCHANTS", f"[{len(noncore_merchant_a)}] match by shortcode", PAD_LENGTH)
    print("Sample Data:")
    print(noncore_merchant_a.head(10))
    print('―' * TERMINAL_WIDTH)

    # 3. RETRIEVE KEYWORDS FROM ACTIVE MERCHANTS
    c_print("✨ FETCHING KEYWORDS FROM NON-CORE SOURCE USING MATCH MERCHANTS", "keywords" , PAD_LENGTH)
    col_keywords = db_noncore.get_collection('keywords')
    keyword_a = fetch(col_keywords, [{
        "$match": {
            "eligibility.merchant": {
                "$in": noncore_merchant_a['_id'].astype(str).tolist()
            }
        }
    }, {
        "$project": {
            "_id": 1,
            "name": "$eligibility.name"
        }
    }], BATCH_SIZE_PROCESS)
    c_print("✨ FOUND KEYWORD", f"[{len(keyword_a)}] keyword(s) found", PAD_LENGTH)
    print("Sample Data:")
    print(keyword_a.head(10))
    print('―' * TERMINAL_WIDTH)

    # 4. RETRIEVE VOCUHER STATISTICS FROM CORE VOUCHER DATABASE
    c_print("✨ FETCHING VOUCHER STAT FROM CORE", "voucher" , PAD_LENGTH)
    keyword_a['parsed'] = keyword_a['name'].astype(str).str.cat(keyword_a['_id'].astype(str), sep='_')
    col_voucher = db_core_voucher.get_collection('voucher')
    voucher_a = fetch(col_voucher, [{
        "$match": {
            "batch_no": {
                "$in": keyword_a['parsed'].astype(str).tolist()
            }
        }
    }, {
        "$sort": {
            "_id": -1
        }
    },
    {
        "$group": {
            "_id": {
                "batch_no": {
                    "$ifNull": ["$batch_no", "Unknown"]
                },
                "status": {
                    "$ifNull": ["$status", "Unknown"]
                }
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$group": {
            "_id": "$_id.batch_no",
            "statuses": {
                "$push": {
                    "k": "$_id.status",
                    "v": "$count"
                }
            }
        }
    },
    {
        "$addFields": {
            "statuses": {
                "$map": {
                    "input": "$statuses",
                    "as": "status",
                    "in": {
                        "k": "$$status.k",
                        "v": {
                            "$cond": {
                                "if": { "$eq": ["$$status.v", "$$status.v"] },
                                "then": "$$status.v",
                                "else": 0
                            }
                        }
                    }
                }
            }
        }
    },
    {
        "$project": {
            "batch_no": "$_id",
            "statusesObject": {
                "$arrayToObject": "$statuses"
            }
        }
    },
    {
        "$replaceRoot": {
            "newRoot": {
                "$mergeObjects": [
                    {
                        "batch_no": "$batch_no"
                    },
                    "$statusesObject"
                ]
            }
        }
    }], BATCH_SIZE_PROCESS)
    voucher_a[['keyword_name', 'keyword_id']] = voucher_a['batch_no'].str.split('_', expand=True)
    c_print("✨ FOUND VOUCHER STAT", "", PAD_LENGTH)
    print("Sample Data:")
    print(voucher_a.head(10))

except Exception as e:
    raise ConnectionError("Unable to find the document due to the following error: ", e) from e
finally:
    c_print("✨ DONE AT ", f"[{(datetime.now() - START_TIME)}] seconds", PAD_LENGTH)
    client.close()