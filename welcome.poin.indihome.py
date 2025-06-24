'''Welcome POIN Indihome v.1.0.0'''
import os
import configparser
import datetime
import pandas as pd

config = configparser.ConfigParser()
config.read('.env')

ENVIRONMENT = config['ENVIRONMENT']['TARGET']
SNAPSHOT_PATH = config['WELCOME_POIN_INDIHOME']['PATH']
SNAPSHOT_FILE_PATTERN = config['WELCOME_POIN_INDIHOME']['FILE_PATTERN']
LOGS = config['WELCOME_POIN_INDIHOME']['LOGGING']
PAD_LENGTH = 100

def print_to_log_file(log_message, log_file_path):
    """Prints a message to the console and appends it to a log file with a timestamp"""
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{current_time}] {log_message}"
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

def validate_file(group, ctl_path):
    '''Validate file with ctl'''
    # Placeholder for validation logic
    print_to_log_file(f'[{group}] Validating CTL', f'{LOGS}/welcome.poin.indihome.{today}.log')
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
                                if(line_count != int(fields[2])):
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

            c_print('✨  Expected record count', f'{total_expected_record_count}' , PAD_LENGTH)
            c_print('✨  Expected file count', f'{total_expected_file_count}' , PAD_LENGTH)
            c_print('✨  File count listed on ctl', f'{total_listed_file_count}' , PAD_LENGTH)
            c_print('✨  Actual file count', f'{len(data)}' , PAD_LENGTH)
            c_print('✨  Actual line count', f'{total_actual_line_count}' , PAD_LENGTH)

            print_to_log_file(c_string(f'[{group}] Expected record count', f'{total_expected_record_count}' , PAD_LENGTH), f'{LOGS}/welcome.poin.indihome.{today}.log')
            print_to_log_file(c_string(f'[{group}] Expected file count', f'{total_expected_file_count}' , PAD_LENGTH), f'{LOGS}/welcome.poin.indihome.{today}.log')
            print_to_log_file(c_string(f'[{group}] File count listed on ctl', f'{total_listed_file_count}' , PAD_LENGTH), f'{LOGS}/welcome.poin.indihome.{today}.log')
            print_to_log_file(c_string(f'[{group}] Actual file count', f'{len(data)}' , PAD_LENGTH), f'{LOGS}/welcome.poin.indihome.{today}.log')
            print_to_log_file(c_string(f'[{group}] Actual line count', f'{total_actual_line_count}' , PAD_LENGTH), f'{LOGS}/welcome.poin.indihome.{today}.log')

            print('\n\n\n')

            allow_process = True

            # Check if the number of files listed in ctl matches the expected count
            if(len(data) != total_expected_file_count):
                c_print('⚠️   Warning : Actual file count is not matched witch CTL', f'CTL:{total_listed_file_count} | Actual:{len(data)}' , PAD_LENGTH)
                allow_process = False

            if(len(data) != total_listed_file_count):
                if(len(not_matched_row) > 0):
                    for obj in not_matched_row:
                        c_print(f'⚠️   Warning : File row not matched : {obj['file_name']}', f'CTL :{obj['record_count']} | Actual:{obj['actual']}' , PAD_LENGTH)

                    allow_process = False

            if(total_expected_record_count != total_actual_line_count):
                c_print('⚠️   Warning : Actual line count is not matched with CTL', f'CTL:{total_expected_record_count} | Actual:{total_actual_line_count}' , PAD_LENGTH)
                allow_process = False

                

            print('\n\n\n')

            if(allow_process):
                return matched_row
            else:
                return []
            
    except FileNotFoundError:
        c_print('⚠️   CTL not found', '!!!' , PAD_LENGTH)
        return None

# 1. Validate file with ctl
today = datetime.date.today().strftime('%Y%m%d')
result = validate_file(f'{SNAPSHOT_FILE_PATTERN}{today}.ctl', f'{SNAPSHOT_PATH}/{SNAPSHOT_FILE_PATTERN}{today}.ctl')

# 2. Process data
if(len(result) > 0):
    print_to_log_file(f'[{SNAPSHOT_FILE_PATTERN}{today}.ctl] CTL file validation passed. Proceeding with data processing.', f'{LOGS}/welcome.poin.indihome.{today}.log')
    for obj in result:
        df = pd.read_csv(f'{SNAPSHOT_PATH}/{obj["file_name"]}', sep='|')
        print(df)
else:
    print_to_log_file(f'[{SNAPSHOT_FILE_PATTERN}{today}.ctl] CTL file validation failed. Exiting.', f'{LOGS}/welcome.poin.indihome.{today}.log')

# 3. Summary Result