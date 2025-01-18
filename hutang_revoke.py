"""HUTANG REVOKE POINT V1"""
import urllib.parse
import configparser
import os
from datetime import datetime
import http.client
import json
from dateutil import tz
import pandas as pd
from tabulate import tabulate

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
        return log_data[['requestor_msisdn', 'poin_revoke', 'filename']]
    except (ValueError, TypeError) as e:
        print(f"Error loading log file: {e.__class__.__name__} - {e}")

def get_balance(msisdn):
    """Load"""
    conn = http.client.HTTPConnection(f"{config['NONCORE']['URL']}", 3000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f"Bearer {config['REVOKE']['TOKEN']}"
        }
    conn.request('GET', f"/v1/{msisdn}/point?bucket_type=TelkomselPOIN", headers=headers)
    response = conn.getresponse()
    data = response.read().decode()
    conn.close()
    return data

def redeem(msisdn, keyword, total_redeem):
    """Load"""
    conn = http.client.HTTPConnection(config['NONCORE']['URL'])
    data = {
        "locale": "en-US",
        "msisdn": msisdn,
        "channel_id" : config['REVOKE']['CHANNEL'],
        "keyword": keyword,
        "total_redeem": total_redeem,
        "send_notification": "false"
    }
    json_data = json.dumps(data)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f"Bearer {config['REVOKE']['TOKEN']}"
        }
    conn.request('POST', '/v1/redeem', json_data, headers)
    response = conn.getresponse()
    return json.dumps({
        'status': response.status,
        'payload': response.read()
    })

def process_data(df):
    """Load"""
    try:
        df_1 = df
        file_logs = df_1['filename'].unique()
        for log in file_logs:
            target_file_name = f"logs/{log.replace('.csv', '.log')}"
            with open(target_file_name, "w", encoding="UTF-8") as log_file:
                log_file.write("MSISDN|KEYWORD|CURRENT_BALANCE|REVOKE_POINT|TOTAL_REDEEM|TRX_NO|CHANNEL_ID|FILE_NAME|ROW_NUMBER|IS_PROCESS|TIMESTAMP|MESSAGE\n")
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
                balance_data = json.loads(get_balance(msisdn))
                if('list_of_point' in balance_data['payload'] and len(balance_data['payload']['list_of_point']) > 0):
                    balance = int(balance_data['payload']['list_of_point'][0]['total_point'])
                else:  
                    balance = 0
            except json.JSONDecodeError:
                message = 'Invalid JSON balance data'

            # Check Balance
            
            # if(balance >= revoke):
            #     total_redeem = revoke
            # elif(balance <= revoke):
            #     total_redeem = balance
            # else:
            #     total_redeem = 0

            total_redeem = revoke

            channel = config['REVOKE']['CHANNEL']
            filename = row.filename
            row_number = (row.Index + 1)
            is_process = "false"
            timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
            message = "-"
            trx = "-"

            with open(log_to_file, "a", encoding="UTF-8") as log_file:
                log_file.write(f"{msisdn}|{keyword}|{balance}|{revoke}|{total_redeem}|{trx}|{channel}|{filename}|{row_number}|{is_process}|{timestamp}|{message}\n")
                log_file.flush()

    except (ValueError, TypeError) as e:
        print(f"Error loading log file: {e.__class__.__name__} - {e}")

def tabulate_log_data(log_data):
    """Load"""
    try:
        tabulated_data = tabulate(log_data, headers='keys', tablefmt='psql')
        return tabulated_data
    except (ValueError, TypeError) as e:
        print(f"Error tabulating log data: {e}")

def main():
    """Load"""
    directory = "revoke"
    file_list = [f.name for f in os.scandir(directory)]
    log_data = load_log_file(file_list, directory)
    
    if log_data is not None:
        process_data(log_data)

if __name__ == "__main__":
    main()