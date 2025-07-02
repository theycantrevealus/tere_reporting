import http.client
import json
import datetime
import os
import shutil
from pymongo import MongoClient

async def set_refresh_token(config):
    """Load"""
    conn = http.client.HTTPConnection(config['NONCORE']['URL'], config['NONCORE']['PORT'])
    data = {
        "type": "user",
        "username": f"{config['NONCORE']['USERNAME']}",
        "password" : f"{config['NONCORE']['PASSWORD']}",
        "client_id": f"{config['NONCORE']['CLIENT_ID']}",
        "client_secret": f"{config['NONCORE']['CLIENT_SECRET']}"
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
    conn.close()
    if(status_code == 200):
        login_data = json.loads(data)
        access_token = login_data['access_token']
        return access_token, ""
    return "", data

# return redeem result and error flag
async def get_url(flag, url_path, auth = ""):
    """Load"""
    conn = http.client.HTTPConnection(url_path)
    if flag == 'authentication':
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    else:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f"Bearer {auth}"
        }

    conn.request('GET', url_path, headers=headers)
    response = conn.getresponse()
    status_code = response.status
    if status_code in (200,201,202):
        data = response.read().decode()
        return json.loads(data), False
    return f"Unhandled response [{flag}] :: {status_code} - {response.read().decode()}", True

# return redeem result and error flag
async def redeem(config, log_file_path, url, port, url_path, token, msisdn, keyword, channel, transaction_id = None, total_redeem = -1, total_bonus = 0, send_notification = False):
    """Load"""
    conn = http.client.HTTPConnection(url, port)
    data = {
        "locale": "en-US",
        "msisdn": msisdn,
        "channel_id" : channel,
        "keyword": keyword,
        "send_notification": send_notification
    }

    if transaction_id:
        data.update({'transaction_id': transaction_id})
    if total_redeem > -1:
        data.update({'total_redeem': total_redeem})
    if total_bonus > 0:
        data.update({'total_bonus': total_bonus})

    json_data = json.dumps(data)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f"Bearer {token}"
    }

    print_trx_to_log_file(log_file_path, 'redeem', msisdn, transaction_id, f"Hit Redeem API {url_path} using payload : {json_data}")
    conn.request('POST', url_path, json_data, headers)
    response = conn.getresponse()
    status_code = response.status
    if status_code in (200, 202):
        data = response.read().decode()
        print_trx_to_log_file(log_file_path, 'redeem', msisdn, transaction_id, f"Success hit Redeem API {url_path}, response : {data}")
        return json.loads(data), False
    elif status_code in (403, 401):
        access_token, error_msg = await set_refresh_token(config)
        if error_msg != "":
            return f"Unhandled response refresh auth [AUTHENTICATION] :: {error_msg}", True

        print_trx_to_log_file(log_file_path, 'authentication', msisdn, transaction_id, f"Try to refresh authentication, response : {data}")
        return await redeem(config, log_file_path, url, port, url_path, access_token, msisdn, keyword, channel, transaction_id, total_redeem, total_bonus, send_notification), False
    else:
        return f"Unhandled response [REDEEM] :: {status_code} - {response.read().decode()}", True

def check_transaction_exists(mongo_uri, database, collection, query_filter):
    """Load"""
    client = MongoClient(mongo_uri)
    database = client.get_database(database)
    collection = database.get_collection(collection)
    return collection.find_one(query_filter) is not None

def print_to_log_file(log_file_path, log_message):
    """Prints a message to the console and appends it to a log file with a timestamp"""
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"{current_time} | {log_message}"
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(log_message + "\n")

def print_trx_to_log_file(log_file_path, activity, service_id, channel_trx_id, message):
    """Prints a message to the console and appends it to a log file with a timestamp by specific format"""
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"{current_time} | {activity} | {service_id} | {channel_trx_id} | {message}"
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(log_message + "\n")

async def get_all_data(mongo_uri, database, collection, query_filter):
    """General function for load data detail"""
    client = MongoClient(mongo_uri)
    database = client.get_database(database)
    collection = database.get_collection(collection)
    return collection.find(query_filter)

def get_data_detail(mongo_uri, database, collection, query_filter, projection=None):
    """General function for load data detail"""
    client = MongoClient(mongo_uri)
    database = client.get_database(database)
    collection = database.get_collection(collection)
    return collection.find_one(query_filter, projection)

def insert_data(mongo_uri, database, collection, data):
    """General function for insert data"""
    client = MongoClient(mongo_uri)
    database = client.get_database(database)
    collection = database.get_collection(collection)
    insert_result = collection.insert_one(data)
    return insert_result

def update_data(mongo_uri, database, collection, query_filter, data):
    """General function for update data"""
    client = MongoClient(mongo_uri)
    database = client.get_database(database)
    collection = database.get_collection(collection)
    update_result = collection.update_one(query_filter, {"$set": data})
    return update_result

def allowed_msisdn(msisdn):
    """Check if msisdn is allowed"""
    prefixes = ("08", "62", "81", "82", "83", "85", "628")
    return any(msisdn.startswith(prefix) and msisdn[len(prefix):].isdigit() for prefix in prefixes)

def scan_file_name_os_listdir(directory, extension):
    """Scan files with specific extension using os.listdir()"""
    files = []
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            if filename.lower().endswith(extension.lower()):
                name_without_ext = os.path.splitext(filename)[0]
                files.append(name_without_ext)
    return files

def move_specific_file(log_file_path, flag, source_dir, destination_dir, filename):
    """Move specific files to destination directory"""
    os.makedirs(destination_dir, exist_ok=True)
    moved_file = ""
    source_path = os.path.join(source_dir, filename)
    destination_path = os.path.join(destination_dir, filename)
    if os.path.exists(source_path):
        try:
            shutil.move(source_path, destination_path)
            print_to_log_file(log_file_path, f"{flag} Moved to {destination_dir}: {filename}")
            moved_file = filename
        except Exception as e:
            print_to_log_file(log_file_path, f"{flag} Error moving {filename}: {e}")
    else:
        print_to_log_file(log_file_path, f"{flag} File not found in {source_dir}: {filename}")
    return moved_file

def simple_confirmation(message="Do you want to continue?"):
    """Simple confirmation with custom message"""
    while True:
        response = input(f"{message} (y/n): ").lower().strip()
        if response in ['y', 'yes', '1', 'true']:
            return True
        elif response in ['n', 'no', '0', 'false']:
            return False
        else:
            print("Invalid input. Please enter 'y' for yes or 'n' for no.")