"""REVOKE POINT V1"""
import urllib.parse
import configparser
import os
from datetime import datetime
import http.client
import json
import asyncio
from pymongo import MongoClient
from dateutil import tz
import pandas as pd

config = configparser.ConfigParser()
config.read('.env')

ENVIRONMENT = config['ENVIRONMENT']['TARGET']
if ENVIRONMENT == 'development':
    MONGO_URI = "mongodb://" + config['MONGO']['HOST'] + "/"
else:
    MONGO_URI = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']

def load_log_file(file_path, directory):
    """Load"""
    try:
        log_data = pd.concat([pd.read_csv(f"{directory}/{file}", sep='|').assign(filename=os.path.basename(file)) for file in file_path], ignore_index=False)
        log_data['requestor_msisdn'] = log_data['requestor_msisdn'].astype(str)
        return log_data[['serial_no', 'requestor_msisdn', 'poin_revoke', 'filename']]
    except (ValueError, TypeError) as e:
        print(f"Error loading log file: {e.__class__.__name__} - {e}")

async def wait_transaction_finish(trx_id):
    """Load"""
    client = MongoClient(MONGO_URI)
    database = client.get_database(config['MONGO']['DATABASE'])
    collection = database.get_collection(f"{config['MONGO']['COL_TRX_MASTER']}")
    while True:
        data = collection.find_one({ "transaction_id": trx_id, "origin": {"$regex": "deduct_" } })
        if data:
            break
        print(f"Waiting for transaction [{trx_id}] to finish")
        await asyncio.sleep(int(config['REVOKE']['DELAY']))

async def get_balance(msisdn):
    """Load"""
    conn = http.client.HTTPConnection(f"{config['NONCORE']['URL']}", config['NONCORE']['PORT'])
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f"Bearer {config['REVOKE']['TOKEN']}"
        }
    conn.request('GET', f"/v2/{msisdn}/poin?bucket_type=TelkomselPOIN&identifier=MSISDN", headers=headers)
    response = conn.getresponse()
    data = response.read().decode()
    conn.close()
    return data

async def redeem(msisdn, keyword, total_redeem, channel):
    """Load"""
    conn = http.client.HTTPConnection(config['NONCORE']['URL'], config['NONCORE']['PORT'])
    data = {
        "locale": "en-US",
        "msisdn": msisdn,
        "channel_id" : channel,
        "keyword": keyword,
        "total_redeem": total_redeem,
        "send_notification": False
    }
    json_data = json.dumps(data)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f"Bearer {config['REVOKE']['TOKEN']}"
        }
    conn.request('POST', '/v1/redeem', json_data, headers)
    response = conn.getresponse()
    data = response.read().decode()
    conn.close()
    return data

def check_transaction_exists(serial_no):
    """Load"""
    client = MongoClient(MONGO_URI)
    database = client.get_database(config['MONGO']['DATABASE_CORE'])
    collection = database.get_collection(f"{config['MONGO']['COL_SERIAL']}")
    return collection.find_one({ "sn": serial_no}) is not None

async def process_data(df):
    """Load"""
    try:
        df_1 = df
        file_logs = df_1['filename'].unique()
        for log in file_logs:
            target_file_name = f"logs/{log.replace('.csv', '.log')}"
            with open(target_file_name, "w", encoding="UTF-8") as log_file:
                log_file.write("MSISDN|KEYWORD|CURRENT_BALANCE|REVOKE_POINT|TOTAL_REDEEM|TRX_NO|CHANNEL_ID|FILE_NAME|ROW_NUMBER|IS_PROCESS|TIMESTAMP|SERIAL_CHECK|MESSAGE\n")
                log_file.flush()

        for row in df.itertuples():
            now = datetime.now(tz=tz.gettz('Asia/Jakarta'))
            msisdn = row.requestor_msisdn
            log_to_file = f"logs/{row.filename.replace('.csv', '.log')}"
            keyword = config['REVOKE']['KEYWORD']
            balance = 0
            revoke = row.poin_revoke

            # GET BALANCE
            try:
                balance_data = json.loads(await get_balance(msisdn))
                if('list_of_point' in balance_data['payload'][0] and len(balance_data['payload'][0]['list_of_point']) > 0):
                    balance = int(balance_data['payload'][0]['list_of_point'][0]['total_point'])
                else:
                    balance = 0
            except json.JSONDecodeError:
                message = 'Invalid JSON balance data'
                balance = 0

            if(balance >= revoke):
                total_redeem = revoke
            elif(balance <= revoke):
                total_redeem = balance
            else:
                total_redeem = 0                
            channel = config['REVOKE']['CHANNEL']
            filename = row.filename
            row_number = (row.Index + 1)
            is_process = "false"
            timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
            message = "-"
            serial = row.serial_no
            check_transaction = check_transaction_exists(serial)
            trx = None

            if ENVIRONMENT == 'development':
                if(total_redeem > 0):
                    message = await redeem(msisdn=msisdn, keyword=keyword, total_redeem=total_redeem, channel=config['REVOKE']['CHANNEL'])
                    redeem_data = json.loads(message)
                    if('payload' in redeem_data and 'trace_id' in redeem_data['payload']):
                        trx = redeem_data['payload']['trace_id']
                    else:
                        trx = None
                else:
                    if(total_redeem <= 0):
                        message = "Skip of redeem 0"
                    else:
                        message = "-"
            else:
                if(total_redeem > 0 and check_transaction):
                    message = await redeem(msisdn=msisdn, keyword=keyword, total_redeem=total_redeem, channel=config['REVOKE']['CHANNEL'])
                    redeem_data = json.loads(message)
                    if('payload' in redeem_data and 'trace_id' in redeem_data['payload']):
                        trx = redeem_data['payload']['trace_id']
                    else:
                        trx = None
                else:
                    if(total_redeem <= 0):
                        message = "Skip of redeem 0"
                    else:
                        message = "-"

            with open(log_to_file, "a", encoding="UTF-8") as log_file:
                log_file.write(f"{msisdn}|{keyword}|{balance}|{revoke}|{total_redeem}|{trx}|{channel}|{filename}|{row_number}|{is_process}|{timestamp}|{check_transaction}|{serial}|{message}\n")
                log_file.flush()

            # Wait transaction to finish
            if(trx is not None):
                await wait_transaction_finish(trx)

            await asyncio.sleep(int(config['REVOKE']['DELAY']))

    except (ValueError, TypeError) as e:
        print(f"Error loading log file: {e.__class__.__name__} - {e}")

async def main():
    """Load"""
    directory = "revoke"
    file_list = [f.name for f in os.scandir(directory)]
    log_data = load_log_file(file_list, directory)    
    if log_data is not None:
        await process_data(log_data)
    await asyncio.sleep(int(config['REVOKE']['DELAY']))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()