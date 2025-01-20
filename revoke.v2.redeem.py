"""REVOKE POINT V1"""
import urllib.parse
import configparser
import os
import subprocess
import http.client
import json
import asyncio
from datetime import datetime
from dateutil import (parser, tz)
from pymongo import MongoClient
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

def report_transaction():
    """Load"""
    client = MongoClient(MONGO_URI)
    database = client.get_database(config['MONGO']['DATABASE_CORE'])
    collection = database.get_collection(f"{config['MONGO']['COL_TRANSACTION']}")
    return pd.DataFrame(list(collection.aggregate([
        {
            "$match": {
                "remark": "Koreksi Poin|ADJPOIN2025|Others",
                "create_time": {
                    "$gte": parser.isoparse("2025-01-19T14:08:00.000Z")
                }
            }
        },
        {
            "$group": {
                "_id": "null",
                "totalPoin": {
                    "$sum": "$amount"
                },
                "rowCount": {
                    "$sum": 1
                }
            }
        }
    ])))

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

        # Process Report
        report_entry = []
        report_headers = [
            "file",
            "success_row",
            "success_total_redeem",
            "success_revoke",
            "fail_row",
            "fail_total_redeem",
            "fail_revoke",
            "partial_row",
            "partial_total_redeem",
            "partial_revoke",
            "source_row",
            "source_revoke",
            "db_row",
            "db_total_redeem"
        ]
        for log in file_logs:
            target_source = f"revoke/{log}"
            target_log = f"logs/{log.replace('.csv', '.log')}"

            # tail -n +2 namafile | awk -F '|' '$3 >= $4 {print}' | wc -l
            # success_row = subprocess.run(["tail", "-n", "+2", target_log, "|", "awk", "-F", "'|'", "'$3>=$4{print}'", "|", "wc", "-l"], shell=True, capture_output=True, text=True, check=True)
            success_row_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            success_row_p2 = subprocess.Popen(["awk", "-F", "|", "$3>=$4{print}"], stdin=success_row_p1.stdout, stdout=subprocess.PIPE)
            success_row_p1.stdout.close()
            success_row_p3 = subprocess.Popen(["wc", "-l"], stdin=success_row_p2.stdout, stdout=subprocess.PIPE)
            success_row_p2.stdout.close()
            success_row = success_row_p3.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | awk -F '|' '$3 >= $4 {print}' | awk -F '{total += $5}END{print total}'
            # success_total_redeem = subprocess.run(["tail", "-n", "+2", target_log, "|", "awk", "-F", "'|'", "'$3>=$4{print}'", "|", "awk", "-F", "'{total+=$5}END{print total}'"], capture_output=True, text=True, check=True)
            success_total_redeem_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            success_total_redeem_p2 = subprocess.Popen(["awk", "-F", "|", "$3>=$4{print}"], stdin=success_total_redeem_p1.stdout, stdout=subprocess.PIPE)
            success_total_redeem_p1.stdout.close()
            success_total_redeem_p3 = subprocess.Popen(["awk", "-F", "|", "{total+=$5}END{print total}"], stdin=success_total_redeem_p2.stdout, stdout=subprocess.PIPE)
            success_total_redeem_p2.stdout.close()
            success_total_redeem = success_total_redeem_p3.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | awk -F '|' '$3 >= $4 {print}' | awk -F '{total += $4}END{print total}'
            # success_revoke = subprocess.run(["tail", "-n", "+2", target_log, "|", "awk", "-F", "'|'", "'$3>=$4{print}'", "|", "awk", "-F", "'{total+=$4}END{print total}'"], capture_output=True, text=True, check=True)
            success_revoke_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            success_revoke_p2 = subprocess.Popen(["awk", "-F", "|", "$3>=$4{print}"], stdin=success_revoke_p1.stdout, stdout=subprocess.PIPE)
            success_revoke_p1.stdout.close()
            success_revoke_p3 = subprocess.Popen(["awk", "-F", "|", "{total+=$4}END{print total}"], stdin=success_revoke_p2.stdout, stdout=subprocess.PIPE)
            success_revoke_p2.stdout.close()
            success_revoke = success_revoke_p3.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | grep Skip | wc -l
            # fail_row = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "wc", "-l"], capture_output=True, text=True, check=True)
            fail_row_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            fail_row_p2 = subprocess.Popen(["grep", "Skip"], stdin=fail_row_p1.stdout, stdout=subprocess.PIPE)
            fail_row_p1.stdout.close()
            fail_row_p3 = subprocess.Popen(["wc", "-l"], stdin=fail_row_p2.stdout, stdout=subprocess.PIPE)
            fail_row = fail_row_p3.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | grep Skip | awk -F '{total += $5}END{print total}'
            # fail_total_redeem = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'{total+=$5}END{print total}'"], capture_output=True, text=True, check=True)
            fail_total_redeem_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            fail_total_redeem_p2 = subprocess.Popen(["grep", "Skip"], stdin=fail_total_redeem_p1.stdout, stdout=subprocess.PIPE)
            fail_total_redeem_p1.stdout.close()
            fail_total_redeem_p3 = subprocess.Popen(["awk", "-F", "|", "{total+=$5}END{print total}"], stdin=fail_total_redeem_p2.stdout, stdout=subprocess.PIPE)
            fail_total_redeem = fail_total_redeem_p3.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | grep Skip | awk -F '{total += $4}END{print total}'
            # fail_revoke = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'{total+=$4}END{print total}'"], capture_output=True, text=True, check=True)
            fail_revoke_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            fail_revoke_p2 = subprocess.Popen(["grep", "Skip"], stdin=fail_revoke_p1.stdout, stdout=subprocess.PIPE)
            fail_revoke_p1.stdout.close()
            fail_revoke_p3 = subprocess.Popen(["awk", "-F", "|", "{total+=$4}END{print total}"], stdin=fail_revoke_p2.stdout, stdout=subprocess.PIPE)
            fail_revoke_p2.stdout.close()
            fail_revoke = fail_revoke_p3.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | grep -v Skip | awk -F '|' '$3 < $4 {print}' | wc -l
            # partial_row = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'|'", "'$3<$4 {print}'", "|", "wc", "-l"], capture_output=True, text=True, check=True)
            partial_row_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            partial_row_p2 = subprocess.Popen(["grep", "Skip"], stdin=partial_row_p1.stdout, stdout=subprocess.PIPE)
            partial_row_p1.stdout.close()
            partial_row_p3 = subprocess.Popen(["awk", "-F", "|", "$3<$4 {print}"], stdin=partial_row_p2.stdout, stdout=subprocess.PIPE)
            partial_row_p2.stdout.close()
            partial_row_p4 = subprocess.Popen(["wc", "-l"], stdin=partial_row_p3.stdout, stdout=subprocess.PIPE)
            partial_row_p3.stdout.close()
            partial_row = partial_row_p4.communicate()[0].decode('utf-8') or 0
            # =========================================================================
            
            # tail -n +2 namafile | grep -v Skip | awk -F '|' '$3 < $4 {print}' | awk -F '{total += $5}END{print total}'
            # partial_total_redeem = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'|'", "'$3<$4 {print}'", "|", "awk", "-F", "'{total+=$5}END{print total}'"], capture_output=True, text=True, check=True)
            partial_total_redeem_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            partial_total_redeem_p2 = subprocess.Popen(["grep", "Skip"], stdin=partial_total_redeem_p1.stdout, stdout=subprocess.PIPE)
            partial_total_redeem_p1.stdout.close()
            partial_total_redeem_p3 = subprocess.Popen(["awk", "-F", "|", "$3<$4 {print}"], stdin=partial_total_redeem_p2.stdout, stdout=subprocess.PIPE)
            partial_total_redeem_p2.stdout.close()
            partial_total_redeem_p4 = subprocess.Popen(["awk", "-F", "|", "{total+=$5}END{print total}"], stdin=partial_total_redeem_p3.stdout, stdout=subprocess.PIPE)
            partial_total_redeem_p3.stdout.close()
            partial_total_redeem = partial_total_redeem_p4.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | grep -v Skip | awk -F '|' '$3 < $4 {print}' | awk -F '{total += $4}END{print total}'
            # partial_revoke = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'|'", "'$3<$4 {print}'", "|", "awk", "-F", "'{total+=$4}END{print total}'"], capture_output=True, text=True, check=True)
            partial_revoke_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
            partial_revoke_p2 = subprocess.Popen(["grep", "Skip"], stdin=partial_revoke_p1.stdout, stdout=subprocess.PIPE)
            partial_revoke_p1.stdout.close()
            partial_revoke_p3 = subprocess.Popen(["awk", "-F", "|", "$3<$4 {print}"], stdin=partial_revoke_p2.stdout, stdout=subprocess.PIPE)
            partial_revoke_p2.stdout.close()
            partial_revoke_p4 = subprocess.Popen(["awk", "-F", "|", "{total+=$4}END{print total}"], stdin=partial_revoke_p3.stdout, stdout=subprocess.PIPE)
            partial_revoke_p3.stdout.close()
            partial_revoke = partial_revoke_p4.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | wc -l
            # source_row = subprocess.run(["tail", "-n", "+2", target_source, "|", "wc", "-l"], capture_output=True, text=True, check=True)
            source_row_p1 = subprocess.Popen(["tail", "-n", "+2", target_source], stdout=subprocess.PIPE)
            source_row_p2 = subprocess.Popen(["wc", "-l"], stdin=source_row_p1.stdout, stdout=subprocess.PIPE)
            source_row_p1.stdout.close()
            source_row = source_row_p2.communicate()[0].decode('utf-8') or 0
            # =========================================================================

            # tail -n +2 namafile | awk -F '|' '{total += $8}END{print total}'
            # source_revoke = subprocess.run(["tail", "-n", "+2", target_source, "|", "awk", "-F", "'|'", "'{total+=$8}END{print total}'"], capture_output=True, text=True, check=True)
            source_revoke_p1 = subprocess.Popen(["tail", "-n", "+2", target_source], stdout=subprocess.PIPE)
            source_revoke_p2 = subprocess.Popen(["awk", "-F", "|", "{total+=$8}END{print total}"], stdin=source_revoke_p1.stdout, stdout=subprocess.PIPE)
            source_revoke_p1.stdout.close()
            source_revoke = source_revoke_p2.communicate()[0].decode('utf-8') or 0
            # =========================================================================
            check_db = report_transaction()
            if(not check_db.empty):
                db_row = check_db.at[0, 'rowCount']
                db_total_redeem = check_db.at[0, 'totalPoin']
            else:
                db_row = 0
                db_total_redeem = 0

            report_entry.append([
                target_source,
                success_row,
                success_total_redeem,
                success_revoke,
                fail_row,
                fail_total_redeem,
                fail_revoke,
                partial_row,
                partial_total_redeem,
                partial_revoke,
                source_row,
                source_revoke,
                db_row,
                db_total_redeem
            ])

        report_df = pd.DataFrame(report_entry, columns=report_headers)
        report_df = report_df.map(lambda x: x.replace('\n', '') if isinstance(x, str) else x)

        # Print table
        # print(tabulate(report_df, headers=report_headers, tablefmt="fancy_grid"))
        print("+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+")
        print("| [LOG - Success] | [LOG - Success] | [LOG - Success] | [LOG - Fail] | [LOG - Fail] | [LOG - Fail] | [LOG - Partial] | [LOG - Partial] | [LOG - Partial] | [Source] | [Source] | [DB]         | [DB]         |")
        print("| Row             | Total Redeem    | Revoke          | Row          | Total Redeem | Revoke       | Row             | Total Redeem    | Revoke          | Row      | Revoke   | Row          | Total Redeem |")
        print("+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+")
        for row in report_df.itertuples(index=False):
            print(f"| File Name : {row.file:191}|")
            print("+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+")
            print(f"| {row.success_row:15} | {row.success_total_redeem:15} | {row.success_revoke:15} | {row.fail_row:12} | {row.fail_total_redeem:12} | {row.fail_revoke:12} | {row.partial_row:15} | {row.partial_total_redeem:15} | {row.partial_revoke:15} | {row.source_row:8} | {row.source_revoke:8} | {row.db_row:12} | {row.db_total_redeem:12} |")
        print("+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+")
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
