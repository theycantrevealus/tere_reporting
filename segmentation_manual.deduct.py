"""What should i do?"""
import sys
import configparser
import urllib.parse
import re
import time
from datetime import datetime
import multiprocessing
import pandas as pd
import pytz
from bson import ObjectId
from pymongo import MongoClient, UpdateOne

config = configparser.ConfigParser()
config.read('.env')
MONGODB_MAX_POOL_SIZE = 100
MONGODB_WRITE_CONCERN = 1
BATCH_SIZE = int(config["CONFIG"]["BATCH_SIZE"])
ENVIRONMENT = config['ENVIRONMENT']['TARGET']
if ENVIRONMENT == 'development':
    MONGO_URI = "mongodb://" + config['MONGO']['HOST'] + "/"
else:
    MONGO_URI = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']

FILE_LINE_COUNT = 0
FILE_TARGET = str(sys.argv[1])
IDENTIFIER = str(sys.argv[2]).lower()
SEGMENTATION_TYPE = str(sys.argv[3]).lower()
PROGRAM_ID = ObjectId(sys.argv[4])
CREATOR_ID = ObjectId(sys.argv[5])
LOCATION_ID = ObjectId(sys.argv[6])
PROCESS_START = time.time()

MSISDN_REGEX = re.compile(r'^(08|62|81|82|83|85|628)+[0-9]{1,13}$')
INDIHOME_REGEX = re.compile(r'^[1-579][0-9]{11}$')

def service_number_check(service_number: str, identifier: str) -> bool:
    """What should i do?"""
    if identifier == 'msisdn':
        return bool(MSISDN_REGEX.match(service_number))
    else:
        return bool(INDIHOME_REGEX.match(service_number))

def worker(chunk_data, queue_process):
    """What should i do?"""
    chunk_data['counter'] = pd.to_numeric(chunk_data['counter'], errors='coerce').fillna(1).astype('int64')
    chunk_data['match'] = chunk_data['msisdn'].apply(service_number_check, args=(IDENTIFIER,))
    queue_process.put(chunk_data)

# APPROACH 1
# ================================================================================================
def writer(queue_task):
    """What should i do?"""
    try:
        client = MongoClient(
            MONGO_URI,
            maxPoolSize=MONGODB_MAX_POOL_SIZE,
            connectTimeoutMS=30000,
            socketTimeoutMS=60000,
            waitQueueTimeoutMS=30000,
            w=MONGODB_WRITE_CONCERN
        )
        target_collection = "programtemplists"
        database = client.get_database(config['MONGO']['DATABASE'])
        collection = database.get_collection(f"{target_collection}")
        while True:
            chunk_a = queue_task.get()
            if chunk_a is None:
                break
            data = chunk_a.to_dict(orient='records')
            bulk_operations = [
                UpdateOne({
                    "msisdn": record['msisdn'],
                    "identifier": IDENTIFIER,
                    "type": SEGMENTATION_TYPE,
                    "program": PROGRAM_ID
                }, {
                    "$inc": {"counter": record['counter']},
                    "$set": {
                        "updated_at": datetime.now(pytz.utc),
                        "irisan": "y-19",
                        # "counter": record['counter'],
                    },
                    "$setOnInsert": {
                        "account": CREATOR_ID,
                        "location": LOCATION_ID,
                        "identifier": IDENTIFIER,
                        # "counter": 1, # Set 1
                        "program": PROGRAM_ID,
                        "msisdn": record['msisdn'],
                        "created_at": datetime.now(pytz.utc),
                        "deleted_at": None,
                        "type": SEGMENTATION_TYPE,
                        "remark": "manual-19",
                        "__v": 0
                    }
                }, upsert=True)
                for record in data
            ]
            collection.bulk_write(bulk_operations, ordered=False)

            # Update all negative value to 0
            count_data = collection.count_documents({
                "identifier": IDENTIFIER,
                "type": SEGMENTATION_TYPE,
                "program": PROGRAM_ID
            })
            print(f'Cleaning [{count_data}] data')
            collection.update_many(filter = {
                "identifier": IDENTIFIER,
                "type": SEGMENTATION_TYPE,
                "program": PROGRAM_ID,
                "counter": {"$lt": 0}
            }, update = {
                "$set": { "counter": 0 }
            })
    except Exception as e:
        raise RuntimeError("Unable to find the document due to the following error: ", e) from e
    finally:
        client.close()

if __name__ == '__main__':
    queue = multiprocessing.Manager().Queue()
    writer_process = multiprocessing.Process(target=writer, args=(queue,))
    writer_process.start()
    chunks = pd.read_csv(FILE_TARGET, sep='|', header=None, names=['msisdn', 'counter'], dtype=str, chunksize=BATCH_SIZE, engine='c')
    chunk_list = [chunk for chunk in chunks]
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.starmap(worker, [(chunk, queue) for chunk in chunk_list])
        pool.close()
        pool.join()
    queue.put(None)
    writer_process.join()
    elapsed = time.time() - PROCESS_START
    print(f'--- DONE : [{elapsed:.3f}] seconds ---')

# APPROACH 2
# ================================================================================================
# def worker(chunk_data, queue_process):
#     """What should i do?"""
#     chunk_data['counter'] = pd.to_numeric(chunk_data['counter'], errors='coerce').fillna(1).astype('int64')
#     chunk_data['match'] = chunk_data['msisdn'].apply(service_number_check, args=(IDENTIFIER,))
#     return chunk_data
# 
# try:
#     os.environ["LIBMONGOC_URI_PARSER_ALLOW_UTF8"] = "true"
#     os.environ["MONGOC_ENABLE_COMPRESSION"] = "snappy,zlib,zstd"
#     client = MongoClient(
#         MONGO_URI,
#         maxPoolSize=MONGODB_MAX_POOL_SIZE,
#         connectTimeoutMS=30000,
#         socketTimeoutMS=60000,
#         waitQueueTimeoutMS=30000,
#         w=MONGODB_WRITE_CONCERN
#     )
#     TARGET_COLLECTION = "programtemplists"
#     database = client.get_database(config['MONGO']['DATABASE'])
#     collection = database.get_collection(f"{TARGET_COLLECTION}")
#     with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
#         chunks = pd.read_csv(FILE_TARGET, sep='|', header=None, names=['msisdn', 'counter'], dtype=str, chunksize=BATCH_SIZE, engine='c')
#         processed_chunks = pool.imap(worker, chunks)
#         for chunk in processed_chunks:
#             data = chunk.to_dict(orient='records')
#             bulk_operations = [UpdateOne({
#                 "msisdn": record['msisdn'],
#                 "identifier": IDENTIFIER,
#                 "type": SEGMENTATION_TYPE,
#                 "program": PROGRAM_ID
#             }, {
#                 "$inc": { "counter": record['counter'] },
#                 "$set": {
#                     "updated_at": datetime.now(pytz.utc),
#                     "match": record['match']
#                 },
#                 "$setOnInsert": {
#                     "account": CREATOR_ID,
#                     "location": LOCATION_ID,
#                     "identifier": IDENTIFIER,
#                     "program": PROGRAM_ID,
#                     "msisdn": record['msisdn'],
#                     "created_at": datetime.now(pytz.utc),
#                     "deleted_at": None,
#                     "type": SEGMENTATION_TYPE,
#                     "__v": 0
#                 }
#             }, upsert=True) for record in data]

#             collection.bulk_write(bulk_operations, ordered=False)
# except Exception as e:
#     raise RuntimeError("Unable to find the document due to the following error: ", e) from e
# finally:
#     client.close()
#     elapsed = time.time() - PROCESS_START
#     print(f'--- DONE : [{elapsed:.3f}] seconds ---')

