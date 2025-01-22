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

class Revoke:
    """Load"""
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('.env')

        self.environment = self.config['ENVIRONMENT']['TARGET']
        self.access_token = ""
        self.refresh_token = ""

        if self.environment == 'development':
            self.mongo_uri = "mongodb://" + self.config['MONGO']['HOST'] + "/"
        else:
            self.mongo_uri = "mongodb://" + self.config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(self.config['MONGO']['PASSWORD']) + "@" + self.config['MONGO']['HOST'] + "/?" + self.config['MONGO']['EXTRA']

    async def process_data(self):
        """Load"""
        try:
            directory = "revoke"
            file_list = [f.name for f in os.scandir(directory)]
            log_data = self.load_log_file(file_list, directory)
            if log_data is not None:
                df_1 = log_data
                file_logs = df_1['filename'].unique()
                for log in file_logs:
                    target_file_name = f"logs/{log.replace('.csv', '.log')}"
                    with open(target_file_name, "w", encoding="UTF-8") as log_file:
                        log_file.write("MSISDN|KEYWORD|CURRENT_BALANCE|REVOKE_POINT|TOTAL_REDEEM|TRX_NO|CHANNEL_ID|FILE_NAME|ROW_NUMBER|IS_PROCESS|TIMESTAMP|SERIAL_CHECK|MESSAGE\n")
                        log_file.flush()

                for row in log_data.itertuples():
                    now = datetime.now(tz=tz.gettz('Asia/Jakarta'))
                    msisdn = row.requestor_msisdn
                    log_to_file = f"logs/{row.filename.replace('.csv', '.log')}"
                    keyword = self.config['REVOKE']['KEYWORD']
                    balance = 0
                    total_redeem = 0
                    revoke = row.poin_revoke

                    # GET BALANCE
                    try:
                        balance_data = json.loads(await self.get_balance(msisdn))
                        if('list_of_point' in balance_data['payload'][0] and len(balance_data['payload'][0]['list_of_point']) > 0):
                            balance = int(balance_data['payload'][0]['list_of_point'][0]['total_point'])
                        else:
                            balance = 0
                    except json.JSONDecodeError:
                        message = 'Invalid JSON balance data'
                        balance = 0

                    # 9930 <> 9330
                    if(balance >= revoke):
                        total_redeem = revoke
                    elif(balance <= revoke):
                        total_redeem = balance
                    else:
                        total_redeem = 0
                    channel = self.config['REVOKE']['CHANNEL']
                    filename = row.filename
                    row_number = (row.Index + 1)
                    is_process = "false"
                    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
                    message = "-"
                    serial = row.serial_no
                    check_transaction = self.check_transaction_exists(serial)
                    trx = None

                    print(f"Balance              : {balance}")
                    print(f"Revoke               : {revoke}")
                    print(f"Redeem               : {total_redeem}")
                    print(f"Check Serial         : {check_transaction}")
                    print("=================================================================")

                    if self.environment == 'development':
                        if(total_redeem > 0):
                            message = await self.redeem(msisdn=msisdn, keyword=keyword, total_redeem=total_redeem, channel=self.config['REVOKE']['CHANNEL'])
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
                            message = await self.redeem(msisdn=msisdn, keyword=keyword, total_redeem=total_redeem, channel=self.config['REVOKE']['CHANNEL'])
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
                        await self.wait_transaction_finish(trx)

                    await asyncio.sleep(int(self.config['REVOKE']['DELAY']))

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
                    check_db = self.report_transaction()
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
                snapshot_target = "snapshot/result.log"
                await self.write_to_result(content="+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+", target=snapshot_target)
                await self.write_to_result(content="| [LOG - Success] | [LOG - Success] | [LOG - Success] | [LOG - Fail] | [LOG - Fail] | [LOG - Fail] | [LOG - Partial] | [LOG - Partial] | [LOG - Partial] | [Source] | [Source] | [DB]         | [DB]         |", target=snapshot_target)
                await self.write_to_result(content="| Row             | Total Redeem    | Revoke          | Row          | Total Redeem | Revoke       | Row             | Total Redeem    | Revoke          | Row      | Revoke   | Row          | Total Redeem |", target=snapshot_target)
                await self.write_to_result(content="+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+", target=snapshot_target)
                for row in report_df.itertuples(index=False):
                    await self.write_to_result(content=f"| File Name : {row.file:191}|", target=snapshot_target)
                    await self.write_to_result(content="+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+", target=snapshot_target)
                    await self.write_to_result(content=f"| {row.success_row:15} | {row.success_total_redeem:15} | {row.success_revoke:15} | {row.fail_row:12} | {row.fail_total_redeem:12} | {row.fail_revoke:12} | {row.partial_row:15} | {row.partial_total_redeem:15} | {row.partial_revoke:15} | {row.source_row:8} | {row.source_revoke:8} | {row.db_row:12} | {row.db_total_redeem:12} |", target=snapshot_target)
                    await self.write_to_result(content="+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+", target=snapshot_target)
                await self.write_to_result(content="", target=snapshot_target)
                await self.write_to_result(content="", target=snapshot_target)

                # Move file after finish processing


                # print("+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+")
                # print("| [LOG - Success] | [LOG - Success] | [LOG - Success] | [LOG - Fail] | [LOG - Fail] | [LOG - Fail] | [LOG - Partial] | [LOG - Partial] | [LOG - Partial] | [Source] | [Source] | [DB]         | [DB]         |")
                # print("| Row             | Total Redeem    | Revoke          | Row          | Total Redeem | Revoke       | Row             | Total Redeem    | Revoke          | Row      | Revoke   | Row          | Total Redeem |")
                # print("+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+")
                # for row in report_df.itertuples(index=False):
                #     print(f"| File Name : {row.file:191}|")
                #     print("+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+")
                #     print(f"| {row.success_row:15} | {row.success_total_redeem:15} | {row.success_revoke:15} | {row.fail_row:12} | {row.fail_total_redeem:12} | {row.fail_revoke:12} | {row.partial_row:15} | {row.partial_total_redeem:15} | {row.partial_revoke:15} | {row.source_row:8} | {row.source_revoke:8} | {row.db_row:12} | {row.db_total_redeem:12} |")
                #     print("+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+")
            await asyncio.sleep(int(self.config['REVOKE']['DELAY']))
        except (ValueError, TypeError) as e:
            print(f"Error loading log file: {e.__class__.__name__} - {e}")

    def load_log_file(self, file_path, directory):
        """Load"""
        try:
            log_data = pd.concat([pd.read_csv(f"{directory}/{file}", sep='|').assign(filename=os.path.basename(file)) for file in file_path], ignore_index=False)
            log_data['requestor_msisdn'] = log_data['requestor_msisdn'].astype(str)
            return log_data[['serial_no', 'requestor_msisdn', 'poin_revoke', 'filename']]
        except (ValueError, TypeError) as e:
            print(f"Error loading log file: {e.__class__.__name__} - {e}")

    async def wait_transaction_finish(self, trx_id):
        """Load"""
        client = MongoClient(self.mongo_uri)
        database = client.get_database(self.config['MONGO']['DATABASE'])
        collection = database.get_collection(f"{self.config['MONGO']['COL_TRX_MASTER']}")
        while True:
            data = collection.find_one({ "transaction_id": trx_id, "origin": {"$regex": "deduct_" } })
            if data:
                break
            print(f"Waiting for transaction [{trx_id}] to finish")
            await asyncio.sleep(int(self.config['REVOKE']['DELAY']))

    async def set_refresh_token(self):
        """Load"""
        conn = http.client.HTTPConnection(self.config['NONCORE']['URL'], self.config['NONCORE']['PORT'])
        data = {
            "type": "user",
            "username": f"{self.config['NONCORE']['USERNAME']}",
            "password" : f"{self.config['NONCORE']['PASSWORD']}",
            "client_id": f"{self.config['NONCORE']['CLIENT_ID']}",
            "client_secret": f"{self.config['NONCORE']['CLIENT_SECRET']}"
        }
        json_data = json.dumps(data)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
            }
        conn.request('POST', '/v1/oauth/signin', json_data, headers)
        response = conn.getresponse()
        status_code = response.status
        data = response.read().decode()
        if(status_code == 200):
            login_data = json.loads(data)
            self.access_token = login_data['access_token']
            self.refresh_token = login_data['refresh_token']
        else:
            await self.write_to_result(content=data)
        conn.close()

    async def write_to_result(self, content = "", target="apps.log"):
        """Load"""
        with open(target, "a", encoding="UTF-8") as log_file:
            log_file.write(f"{content}\n")
            log_file.flush()

    async def get_balance(self, msisdn):
        """Load"""
        conn = http.client.HTTPConnection(f"{self.config['NONCORE']['URL']}", self.config['NONCORE']['PORT'])
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f"Bearer {self.access_token}"
            }
        conn.request('GET', f"/v2/{msisdn}/poin?bucket_type=TelkomselPOIN&identifier=MSISDN", headers=headers)
        response = conn.getresponse()
        status_code = response.status
        if(status_code == 200):
            data = response.read().decode()
            conn.close()
            return data
        elif(status_code == 403):
            await self.set_refresh_token()
            return await self.get_balance(msisdn)
        else:
            conn.close()
            await self.write_to_result(content=f"Unhandled response [GET BALANCE] :: {status_code} - {response.read().decode()}")

    async def redeem(self, msisdn, keyword, total_redeem, channel):
        """Load"""
        conn = http.client.HTTPConnection(self.config['NONCORE']['URL'], self.config['NONCORE']['PORT'])
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
            'Authorization': f"Bearer {self.access_token}"
            }
        conn.request('POST', '/v1/redeem', json_data, headers)
        response = conn.getresponse()
        status_code = response.status
        if(status_code == 200 or status_code == 202):
            data = response.read().decode()
            return data
        elif(status_code == 403):
            await self.set_refresh_token()
            return await self.redeem(msisdn=msisdn, keyword=keyword, total_redeem=total_redeem, channel=channel)
        else:
            conn.close()
            await self.write_to_result(content=f"Unhandled response [REDEEM] :: {status_code} - {response.read().decode()}")

    def check_transaction_exists(self, serial_no):
        """Load"""
        client = MongoClient(self.mongo_uri)
        database = client.get_database(self.config['MONGO']['DATABASE_CORE'])
        collection = database.get_collection(f"{self.config['MONGO']['COL_SERIAL']}")
        return collection.find_one({ "sn": serial_no}) is not None
    
    def report_transaction(self):
        """Load"""
        client = MongoClient(self.mongo_uri)
        database = client.get_database(self.config['MONGO']['DATABASE_CORE'])
        collection = database.get_collection(f"{self.config['MONGO']['COL_TRANSACTION']}")
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
    
# ========================================================================================

async def main():
    """Load"""
    revoke = Revoke()
    await revoke.process_data()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
