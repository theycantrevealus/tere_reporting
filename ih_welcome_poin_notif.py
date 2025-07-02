'''Welcome POIN Indihome Notification v.1.0.0'''
import configparser
import http.client
import os
import re
import datetime
# import aiohttp
import asyncio
import ssl
import urllib.parse
from utils import get_data_detail, get_all_data, update_data, print_trx_to_log_file

config = configparser.ConfigParser()
config.read('.env')

ENVIRONMENT = config['ENVIRONMENT']['TARGET']
SNAPSHOT_PATH = config['WELCOME_POIN_INDIHOME']['PATH']
SNAPSHOT_FILE_PATTERN = config['WELCOME_POIN_INDIHOME']['FILE_PATTERN']
LOGS = config['WELCOME_POIN_INDIHOME']['LOGGING']
REDEEM_SUCCESS_TYPE = '6319f7e751e92661186160f0'
NOTIF_VIA_SMS = '62ffc2988a01008799e785fe'
ALLOW_MSISDN_CONFIG_KEY = 'SERVICE_NUMBER_FORMAT'
SMS_GATEWAY_HOST = config['NOTIFICATION']['SMS_HOST']
SMS_GATEWAY_PORT = config['NOTIFICATION']['SMS_PORT']
SMS_GATEWAY_PATH = config['NOTIFICATION']['SMS_PATH']
SMS_GATEWAY_USER = config['NOTIFICATION']['SMS_USER']
SMS_GATEWAY_PASS = config['NOTIFICATION']['SMS_PASS']
SMS_GATEWAY_FROM = config['NOTIFICATION']['SMS_FROM']
ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(config['NOTIFICATION']['SMS_CA'])

if not os.path.exists(LOGS):
    os.makedirs(LOGS)

if ENVIRONMENT == 'development':
    MONGO_URI = "mongodb://" + config['MONGO']['HOST'] + "/"
else:
    MONGO_URI = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']

# -----------------------------------------------------------
def is_tsel_msisdn(regex_value, msisdn):
    """Check if msisdn is tsel"""
    compiled_pattern = re.compile(regex_value)
    return bool(compiled_pattern.match(msisdn))

async def send_sms(log_file, data, notif_content):
    """Send SMS"""
    try:
        #async with aiohttp.ClientSession():
        params_str = f"{SMS_GATEWAY_PATH}?user={SMS_GATEWAY_USER}&pass={SMS_GATEWAY_PASS}&from={SMS_GATEWAY_FROM}&to={data['no_handphone']}&text={notif_content}"
        conn = http.client.HTTPSConnection(SMS_GATEWAY_HOST, port=int(SMS_GATEWAY_PORT), context=ssl_context)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        conn.request('GET', params_str, headers=headers)
        response = conn.getresponse()
        status_code = response.status
        if status_code in (200,201,202):
            update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'tere_trx_id': data['tere_trx_id']}, {'notification_status': 'sent'})
            print_trx_to_log_file(log_file, 'sms_gateway', data['service_id'], data['channel_trx_id'], f"SMS gateway result: Code={response.status}; Msg={response.msg}")
            return True
   
        update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'tere_trx_id': data['tere_trx_id']}, {'notification_status': 'sent_failed'})
        print_trx_to_log_file(log_file, 'sms_gateway', data['service_id'], data['channel_trx_id'], f"SMS gateway error: {response.read().decode()}")
        return False
    except Exception as e:
        print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Error processing data: {e.__class__.__name__} - {e}")
        return False

async def process_all_data(log_file):
    """Process data"""
    process_service_id = "config_regex_setup"
    process_channel_trx_id = "config_regex_setup"
    try:
        # get config regex tsel number
        config_tsel_number = get_data_detail(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_SYSTEM_CONFIG'], {"param_key": ALLOW_MSISDN_CONFIG_KEY})
        regex_value = config_tsel_number['param_value']['msisdn']['allow']
        all_data = await get_all_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'redeem_status': 'process', 'notification_status': 'pending'})

        for data in all_data:
            process_service_id = data['service_id']
            process_channel_trx_id = data['channel_trx_id']

            print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], "Start processing notification!")
            print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Data payload: {data}")
            trx_result = get_data_detail(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_TRX_MASTER'], {'transaction_id': data['tere_trx_id']})
            if trx_result is None:
                print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Transaction not found in transaction_master: {data}")
                continue

            if trx_result['status'] == 'Fail':
                update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'tere_trx_id': data['tere_trx_id']}, {'redeem_status': 'fail', 'notification_status': 'not_sent'})
                print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Transaction failed! Transaction in transaction_master was failed: {data}")
                continue

            if trx_result['status'] == 'Success':
                # check is tsel number
                # if tsel number false, update redeem status to success and notif = not_sent
                update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'tere_trx_id': data['tere_trx_id']}, {'redeem_status': 'success'})
                if not is_tsel_msisdn(regex_value, data['no_handphone']):
                    update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'tere_trx_id': data['tere_trx_id']}, {'notification_status': 'not_sent'})
                    print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Notification not sent! Non tsel msisdn: {data}")
                    continue

                # get notif template
                keyword_detail = get_data_detail(MONGO_URI, config['MONGO']['DATABASE'], "keywords", {'eligibility.name': data['keyword']}, {'eligibility': 1, 'notification': 1})
                is_notif_sent = False
                for detail in keyword_detail['notification']:
                    if NOTIF_VIA_SMS in detail['via'] and detail['code_identifier'] == REDEEM_SUCCESS_TYPE:
                        is_notif_sent = True
                        print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Send notification via sms to {data['no_handphone']}")
                        notif_template = detail['notification_content']
                        notif_template = notif_template.replace(config['WELCOME_POIN_INDIHOME']['POIN_NOTIF_TEMPLATE'], str(data['point_earned']))
                        notif_template = urllib.parse.quote(notif_template)
                        await send_sms(log_file, data, notif_template)
                        break

                if not is_notif_sent:
                    update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'tere_trx_id': data['tere_trx_id']}, {'notification_status': 'not_sent'})
                    print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Notification not sent! Notif template not found: {data}")

                print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], "Finish processing notification!")

        await asyncio.sleep(int(config['WELCOME_POIN_INDIHOME']['DELAY']))
    except Exception as e:
        print_trx_to_log_file(log_file, 'notification', process_service_id, process_channel_trx_id, f"Error processing data: {e.__class__.__name__} - {e}")

async def main():
    """Send notification"""
    today = datetime.date.today().strftime('%Y%m%d')
    log_file = f'{LOGS}/notif_{SNAPSHOT_FILE_PATTERN}_{today}.log'
    await process_all_data(log_file)
    await asyncio.sleep(int(config['WELCOME_POIN_INDIHOME']['DELAY']))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()