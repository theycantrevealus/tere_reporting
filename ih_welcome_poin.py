'''Welcome POIN Indihome v.1.0.0'''
# import aiohttp
import asyncio
import configparser
import datetime
import os
import urllib.parse

import pandas as pd

from utils import (get_data_detail, insert_data, move_specific_file,
                   print_trx_to_log_file, redeem, scan_file_name_os_listdir)

config = configparser.ConfigParser()
config.read('.env')

ENVIRONMENT = config['ENVIRONMENT']['TARGET']
SNAPSHOT_PATH = config['WELCOME_POIN_INDIHOME']['PATH']
SNAPSHOT_FILE_PATTERN = config['WELCOME_POIN_INDIHOME']['FILE_PATTERN']
LOGS = config['WELCOME_POIN_INDIHOME']['LOGGING']
PAD_LENGTH = 100
IS_USING_HEADER = config['WELCOME_POIN_INDIHOME']['IS_USING_HEADER']
FILE_CHECKSUM_EXTENSION = config['WELCOME_POIN_INDIHOME']['FILE_CHECKSUM_EXTENSION']
FILE_LOAD_EXTENSION = config['WELCOME_POIN_INDIHOME']['FILE_LOAD_EXTENSION']

# setup project dir
if not os.path.exists(LOGS):
    os.makedirs(LOGS)
if not os.path.exists(f'{SNAPSHOT_PATH}/archive'):
    os.makedirs(f'{SNAPSHOT_PATH}/archive')

if ENVIRONMENT == 'development':
    MONGO_URI = "mongodb://" + config['MONGO']['HOST'] + "/"
else:
    MONGO_URI = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']

# -----------------------------------------------------------
def print_to_log_file(log_message, log_file_path):
    """Prints a message to the console and appends it to a log file with a timestamp"""
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"{current_time} | {log_message}"
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(log_message + "\n")

def print_custom_to_log_file(log_message, log_file_path):
    """Prints a custom message to the console and appends it to a log file without a timestamp"""
    log_message = f"{log_message}"
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(log_message + "\n")

def count_files_with_prefix(directory, prefix):
    '''Count files in a directory with a specific prefix'''
    count = 0
    for file in os.listdir(directory):
        if file.startswith(prefix):
            count += 1
    return count

def c_print(first_string, second_string, total_length=80):
    ''' STATUS REPORT'''
    first_string_length = len(first_string)
    second_string_length = len(second_string)
    padding_length = total_length - first_string_length - second_string_length
    if padding_length > 0:
        padded_string = first_string + '_' * padding_length + second_string
        print(padded_string)
    else:
        print(first_string + ' ' + second_string)

def c_string(first_string, second_string, total_length=80):
    ''' STATUS REPORT'''
    first_string_length = len(first_string)
    second_string_length = len(second_string)
    padding_length = total_length - first_string_length - second_string_length
    if padding_length > 0:
        padded_string = first_string + ' ' * padding_length + second_string
        return padded_string
    else:
        return first_string + ' ' + second_string

def validate_file(group, ctl_path, log_file):
    '''Validate file with ctl'''
    # Placeholder for validation logic
    is_using_header = IS_USING_HEADER == '1' # value 1 = true, 0 = false
    header_total = 0

    print_to_log_file(f'[{group}] Validating CTL', log_file)
    try:
        with open(ctl_path, 'r', encoding='utf-8') as file:
            total_expected_record_count = 0
            total_expected_file_count = 0
            total_listed_file_count = 0
            total_actual_line_count = 0

            lines = file.readlines()
            data = []
            not_matched_row = []
            matched_row = []
            for line in lines:
                line = line.strip()
                fields = line.split('|')
                row_type = fields[0]
                if row_type == 'DataFile':
                    if is_using_header:
                        header_total += 1

                    total_listed_file_count += 1
                    file_path = f'{SNAPSHOT_PATH}/{fields[1]}'
                    try:
                        if os.path.exists(file_path):
                            data.append({
                                'row_type': row_type,
                                'file_name': fields[1],
                                'record_count': int(fields[2])
                            })

                            with open(f'{SNAPSHOT_PATH}/{fields[1]}', 'r', encoding='utf-8') as each_file:
                                line_count = len(each_file.readlines())
                                if is_using_header:
                                    line_count -= 1

                                if line_count != int(fields[2]):
                                    not_matched_row.append({
                                        'row_type': row_type,
                                        'file_name': fields[1],
                                        'record_count': int(fields[2]),
                                        'actual': line_count
                                    })
                                else:
                                    matched_row.append({
                                        'row_type': row_type,
                                        'file_name': fields[1],
                                        'record_count': int(fields[2]),
                                        'actual': line_count
                                    })
                                total_actual_line_count += line_count

                    except FileNotFoundError:
                        print(f'File tidak ditemukan: {file_path}')
                        return None

                elif row_type == 'TotalCount':
                    total_expected_record_count = int(fields[2])
                    total_expected_file_count = int(fields[1])

            # total_actual_line_count -= header_total
            c_print('✨  Expected record count', f'{total_expected_record_count}' , PAD_LENGTH)
            c_print('✨  Expected file count', f'{total_expected_file_count}' , PAD_LENGTH)
            c_print('✨  File count listed on ctl', f'{total_listed_file_count}' , PAD_LENGTH)
            c_print('✨  Actual file count', f'{len(data)}' , PAD_LENGTH)
            c_print('✨  Actual line count', f'{total_actual_line_count}' , PAD_LENGTH)

            print_to_log_file(c_string(f'[{group}] Expected record count', f'{total_expected_record_count}' , PAD_LENGTH), log_file)
            print_to_log_file(c_string(f'[{group}] Expected file count', f'{total_expected_file_count}' , PAD_LENGTH), log_file)
            print_to_log_file(c_string(f'[{group}] File count listed on ctl', f'{total_listed_file_count}' , PAD_LENGTH), log_file)
            print_to_log_file(c_string(f'[{group}] Actual file count', f'{len(data)}' , PAD_LENGTH), log_file)
            print_to_log_file(c_string(f'[{group}] Actual line count', f'{total_actual_line_count}' , PAD_LENGTH), log_file)

            print('\n\n\n')

            allow_process = True

            # Check if the number of files listed in ctl matches the expected count
            if len(data) != total_expected_file_count:
                c_print('⚠️   Warning : Actual file count is not matched witch CTL', f'CTL:{total_listed_file_count} | Actual:{len(data)}' , PAD_LENGTH)
                allow_process = False

            if len(data) != total_listed_file_count:
                if len(not_matched_row) > 0:
                    for obj in not_matched_row:
                        c_print(f'⚠️   Warning : File row not matched : {obj["file_name"]}', f'CTL :{obj["record_count"]} | Actual:{obj["actual"]}' , PAD_LENGTH)

                    allow_process = False

            if total_expected_record_count != total_actual_line_count:
                c_print('⚠️   Warning : Actual line count is not matched with CTL', f'CTL:{total_expected_record_count} | Actual:{total_actual_line_count}' , PAD_LENGTH)
                allow_process = False

            print('\n\n\n')
            if allow_process:
                return matched_row
            else:
                return []

    except FileNotFoundError:
        c_print('⚠️   CTL not found', '!!!' , PAD_LENGTH)
        print_custom_to_log_file(f'[{group}] CTL file validation failed. Exiting.', log_file)
        return None

async def process_data(filename, row, fee_range_config, log_file):
    """Process data"""
    try:
        # check redeem order_id, if already redeem skip (TO BE CONFIRM)
        redeem_result = get_data_detail(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_TRX_MASTER'], {'channel_transaction_id': row['order_id']})
        if redeem_result:
            print_trx_to_log_file(log_file, 'redeem', row['service_id'], row['order_id'], f"Redeem not processed: order_id = {row['order_id']} already processed")
            return

        fee_amount = int(row['fee'])
        if row['process_state'].upper() != 'COMPLETED':
            print_trx_to_log_file(log_file, 'redeem', row['service_id'], row['order_id'], f"Redeem not processed because process_state is not COMPLETED. Row process_state = {row['process_state']}")
            return
        # checking fee amount in range fee
        get_range_fee = fee_range_config['param_value']['static']
        if str(fee_amount) in get_range_fee:
            # execute redeem
            access_token = config['WELCOME_POIN_INDIHOME']['TOKEN']
            print_trx_to_log_file(log_file, 'redeem', row['service_id'], row['order_id'], 'Redeem processing...')
            redeem_result, error = await redeem(config, log_file, config['NONCORE']['URL'], config['NONCORE']['PORT'], config['WELCOME_POIN_INDIHOME']['REDEEM_URL_PATH'], access_token, str(row['service_id']), get_range_fee[str(fee_amount)]['keyword'], config['WELCOME_POIN_INDIHOME']['CHANNEL'], row['order_id'], 0, get_range_fee[str(fee_amount)]['poin_amount'])
            if(error):
                print_trx_to_log_file(log_file, 'redeem', row['service_id'], row['order_id'], f'Redeem failed: {redeem_result}')
            else:
                data = {
                    "source_file": filename,
                    "channel_trx_id": row['order_id'], # channel order_id
                    "tere_trx_id": redeem_result['payload']['trace_id'], # tere trx id
                    "service_id": str(row['service_id']),
                    "fee": fee_amount,
                    "keyword": get_range_fee[str(fee_amount)]['keyword'],
                    "process_state": row['process_state'],
                    "no_handphone": str(row['no_handphone']),
                    "secondary_phone": str(row['secondary_phone']),
                    "point_earned": get_range_fee[str(fee_amount)]['poin_amount'],
                    "redeem_status": "process", # process, completed, not_process (if process_state != "COMPLETED")
                    "notification_status": "pending", # pending, sent, not_sent
                    "process_at": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                }
                insert_result = insert_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], data)
                print_trx_to_log_file(log_file, 'redeem', row['service_id'], row['order_id'], "Success process redeem. Insert task _id: " + str(insert_result.inserted_id))
        else:
            print_trx_to_log_file(log_file, 'redeem', row['service_id'], row['order_id'], f"Redeem not processed: Fee amount for {fee_amount} not configured in config file. Configured fee amount: {list(get_range_fee.keys())}")

        await asyncio.sleep(int(config['WELCOME_POIN_INDIHOME']['DELAY']))
    except Exception as e:
        print_trx_to_log_file(log_file, 'redeem', row['service_id'], row['order_id'], f"Redeem error: {e.__class__.__name__} - {e}")
        print(f"Error processing data: {e.__class__.__name__} - {e}")

async def main():
    """Load Welcome Poin Indihome"""

    try:
        # 1. Validate file with ctl
        today = datetime.date.today().strftime('%Y%m%d')
        validate_ctl_log_file = f'{LOGS}/load_ctl.{today}.log'

        print_custom_to_log_file('\n' + '==' * 20 +' File processing started ' + '==' * 20, validate_ctl_log_file)
        files = scan_file_name_os_listdir(SNAPSHOT_PATH, f'.{FILE_CHECKSUM_EXTENSION}')
        ctl_result = []
        for file in files:
            ctl_log_file = f'{LOGS}/welcome.poin.indihome.{file}.{FILE_CHECKSUM_EXTENSION}.log'
            validate_result = validate_file(f'{file}.{FILE_CHECKSUM_EXTENSION}', f'{SNAPSHOT_PATH}/{file}.{FILE_CHECKSUM_EXTENSION}', ctl_log_file)

            if len(validate_result) > 0:
                print_to_log_file(f'[{file}.{FILE_CHECKSUM_EXTENSION}] Checksum validation passed. Proceeding with data processing.', validate_ctl_log_file)
                ctl_result.append(validate_result)
            else:
                print_to_log_file(f'[{file}.{FILE_CHECKSUM_EXTENSION}] Checksum validation failed. Exiting.', validate_ctl_log_file)

        # 2. Process data
        for result in ctl_result:
            range_config_filter = {
                "param_key": "INDIHOME_WELCOME_POIN_FEE_RANGE"
            }
            range_config = get_data_detail(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_SYSTEM_CONFIG'], range_config_filter)
            for obj in result:
                filename = obj["file_name"]
                trx_log_file = f'{LOGS}/{filename}.log'
                df = pd.read_csv(f'{SNAPSHOT_PATH}/{filename}', sep='|')
                print_custom_to_log_file('\n' + '==' * 20 + f' Start loading {filename} ' + '==' * 20,trx_log_file)
                for _, row in df.iterrows():
                    await process_data(filename, row, range_config, trx_log_file)

                # moving to archived
                move_specific_file(validate_ctl_log_file, f'[{filename}]', SNAPSHOT_PATH, f'{SNAPSHOT_PATH}/archive', filename)
                print_to_log_file(f'[{SNAPSHOT_FILE_PATTERN}{today}.{FILE_CHECKSUM_EXTENSION}] Finish processing {filename}', validate_ctl_log_file)

        # 3. Move CTL file to archive
        for file in files:
            move_specific_file(validate_ctl_log_file, f'[{file}.{FILE_CHECKSUM_EXTENSION}]', SNAPSHOT_PATH, f'{SNAPSHOT_PATH}/archive', f'{file}.{FILE_CHECKSUM_EXTENSION}')

        await asyncio.sleep(int(config['WELCOME_POIN_INDIHOME']['DELAY']))
    except Exception as e:
        print(f"An error occurred: {e.__class__.__name__} - {e}")
        print_to_log_file(f"An error occurred: {e.__class__.__name__} - {e}", validate_ctl_log_file)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()