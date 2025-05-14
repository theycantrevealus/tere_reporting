import pandas as pd
import itertools
import re
import datetime
import time
import signal
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
import configparser
import concurrent.futures
import os
import logging
import sys
from functools import partial

# format running python upload_whitelist.py name_files.txt program_id user_id_noncore location_id whitelist/blacklist

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global flag for interruption
interrupted = False

def handle_interrupt(signum, frame):
    global interrupted
    logger.warning("⚠️ Process interrupted! Finishing current batch and stopping...")
    interrupted = True

# Register signal handlers
signal.signal(signal.SIGINT, handle_interrupt)
signal.signal(signal.SIGTERM, handle_interrupt)

# Load configuration
config = configparser.ConfigParser()
config.read(".env")

# MongoDB Config
MONGO_URI = config["MONGO"]["URI"]
DB_NAME = config["MONGO"]["DATABASE"]
COLLECTION_NAME = config["MONGO"]["COL_PROGRAM_TEMPLIST"]
BATCH_SIZE = int(config["CONFIG"]["BATCH_SIZE"]) * 5  # Increased batch size for better throughput

# File input
FILE_PATH = str(sys.argv[1])

# parameter
PROGRAM_ID = ObjectId(str(sys.argv[2]))
ACCOUNT_ID = ObjectId(str(sys.argv[3]))
LOCATION_ID = ObjectId(str(sys.argv[4]))
TYPE = str(sys.argv[5])

# Connection pool size and write concern settings
MONGODB_MAX_POOL_SIZE = 100
MONGODB_WRITE_CONCERN = 1  # Adjust based on durability needs
MAX_WORKERS = min(32, os.cpu_count() * 4)  # Optimal number of workers

# Precompiled regex patterns for performance
PROGRAM_REGEX = re.compile(r"^((08|628)+(11|12|13|21|22|23|51|52|53))+([0-9]{1,13})$")

# Batching function
def batched(iterable, batch_size):
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, batch_size))
        if not batch:
            break
        yield batch

# Optimize data reading with pandas chunking for large files
def read_data_efficient(file_path, chunk_size=100000):
    if file_path.endswith('.csv'):
        chunks = pd.read_csv(file_path, sep='|', header=None, names=['msisdn', 'counter'], 
                            dtype=str, chunksize=chunk_size)
    else:
        chunks = pd.read_csv(file_path, sep='|', header=None, names=['msisdn', 'counter'], 
                            dtype=str, chunksize=chunk_size)

    now = datetime.datetime.utcnow()
    all_data = []

    for chunk in chunks:
        chunk['msisdn'] = chunk['msisdn'].str.strip()
        chunk['counter'] = pd.to_numeric(chunk['counter'], errors='coerce').fillna(1).astype(int)

        chunk['match'] = chunk['msisdn'].str.match(PROGRAM_REGEX)
        
        all_data.extend(
            zip(chunk['msisdn'], chunk['counter'], itertools.repeat(now), chunk['match'])
        )

    return all_data

# Create MongoDB operations in batch
def create_mongo_operations(batch):
    now = datetime.datetime.utcnow()
    ops = []
    
    for msisdn, counter_val, _, match_status in batch:
        filter_doc = {
            "msisdn": msisdn,
            "identifier": "msisdn",
            "type": TYPE,
            "program": PROGRAM_ID
        }
        
        update_doc = {
            "$inc": {"counter": counter_val},
            "$set": {
                "updated_at": now,
                "match": match_status
            },
            "$setOnInsert": {
                "account": ACCOUNT_ID,
                "location": LOCATION_ID,
                "identifier": "msisdn",
                "program": PROGRAM_ID,
                "msisdn": msisdn,
                "created_at": now,
                "deleted_at": None,
                "type": TYPE,
                "__v": 0
            }
        }
        
        ops.append(UpdateOne(filter_doc, update_doc, upsert=True))
    
    return ops

# Process batch with optimized connection handling
def process_batch(batch, client):
    global interrupted
    if interrupted:
        return 0, 0
    
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    ops = create_mongo_operations(batch)
    if ops:
        try:
            result = collection.bulk_write(
                ops, 
                ordered=False,
                bypass_document_validation=True
            )
            return len(ops), result.upserted_count + result.modified_count
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return len(ops), 0
    return 0, 0

# Process all batches with connection pooling and interrupt handling
def process_all_batches(all_data):
    client = MongoClient(
        MONGO_URI,
        maxPoolSize=MONGODB_MAX_POOL_SIZE,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
        waitQueueTimeoutMS=30000,
        w=MONGODB_WRITE_CONCERN
    )
    
    try:
        batches = list(batched(all_data, BATCH_SIZE))
        total_processed = 0
        total_inserted = 0
        
        for i, batch in enumerate(batches):
            if interrupted:
                # logger.info("Process interrupted. Stopping further processing.")
                break
            
            processed, inserted = process_batch(batch, client)
            total_processed += processed
            total_inserted += inserted
            
            progress = (i + 1) / len(batches) * 100
            # logger.info(f"Progress: {progress:.1f}% - Processed: {total_processed}/{len(all_data)} records")
        
        return total_processed, total_inserted
    finally:
        client.close()

# Parallel data preparation function
def prepare_data_parallel():
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # If file is very large, we could split it into chunks for parallel reading
        return list(read_data_efficient(FILE_PATH))

# Main function
def main():
    start_time = time.time()
    
    # logger.info("📥 Reading and validating data...")
    all_data = prepare_data_parallel()
    
    total_records = len(all_data)
    # logger.info(f"📦 Total valid records to process: {total_records}")
    
    # logger.info(f"🚀 Processing data with {MAX_WORKERS} workers...")
    total_processed, total_inserted = process_all_batches(all_data)
    
    elapsed = time.time() - start_time
    # logger.info(f"\n✅ Done. Records processed: {total_processed}")
    # logger.info(f"📊 Records inserted/updated: {total_inserted}")
    # logger.info(f"⏱️ Time taken: {elapsed:.2f} seconds")
    # logger.info(f"⚡ Speed: {total_records / elapsed:.2f} records/second")
    
    return elapsed, total_records

# Entry point with performance monitoring
def run_optimized_processing():
    # Set environment variables for better MongoDB driver performance
    os.environ["LIBMONGOC_URI_PARSER_ALLOW_UTF8"] = "true"
    os.environ["MONGOC_ENABLE_COMPRESSION"] = "snappy,zlib,zstd"
    
    start_time = time.time()
    # logger.info("Starting optimized data processing...")
    
    # Run main function
    elapsed, total_records = main()
    
    # Display final performance metrics
    # logger.info("\n=== Performance Summary ===")
    # logger.info(f"Total records: {total_records}")
    # logger.info(f"Total time: {elapsed:.2f} seconds")
    print(f'--- DONE : [{elapsed:.3f}] seconds ---')
    # logger.info(f"Throughput: {total_records / elapsed:.2f} records/second")
    
    return elapsed, total_records

if __name__ == "__main__":
    run_optimized_processing()