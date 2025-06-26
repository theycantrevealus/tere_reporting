'''Welcome POIN Indihome v.1.0.0'''
import configparser
import re
import datetime
import aiohttp
import asyncio
import urllib.parse
from utils import get_data_detail, get_all_data, get_url, update_data, print_trx_to_log_file

config = configparser.ConfigParser()
config.read('.env')

ENVIRONMENT = config['ENVIRONMENT']['TARGET']
SNAPSHOT_PATH = config['WELCOME_POIN_INDIHOME']['PATH']
SNAPSHOT_FILE_PATTERN = config['WELCOME_POIN_INDIHOME']['FILE_PATTERN']
LOGS = config['WELCOME_POIN_INDIHOME']['LOGGING']
REDEEM_SUCCESS_TYPE = '6319f7e751e92661186160f0'
NOTIF_VIA_SMS = '62ffc2988a01008799e785fe'
ALLOW_MSISDN_CONFIG_KEY = 'SERVICE_NUMBER_FORMAT'

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
        async with aiohttp.ClientSession():
            params_str = "?user=" + config['NOTIFICATION']['SMS_USER'] + "&pass=" + config['NOTIFICATION']['SMS_PASS'] + "&from=" + config['NOTIFICATION']['SMS_FROM'] + "&to=" + data['no_handphone'] + "&text=" + notif_content
            url_path = f"{config['NOTIFICATION']['SMS_HOST']}/{params_str}"
            sms_result, is_error = await get_url('SMS', url_path)
            if is_error:
                print_trx_to_log_file(log_file, 'sms_gateway', data['service_id'], data['channel_trx_id'], f"SMS gateway error: {sms_result}")
            else:
                update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'service_id': data['service_id'], 'channel_trx_id': data['channel_trx_id']}, {'notification_status': 'sent'})
                print_trx_to_log_file(log_file, 'sms_gateway', data['service_id'], data['channel_trx_id'], f"SMS gateway result: {sms_result}")
    except Exception as e:
        print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Error processing data: {e.__class__.__name__} - {e}")

async def process_all_data(log_file):
    """Process data"""
    process_service_id = "config_regex_setup"
    process_channel_trx_id = "config_regex_setup"
    try:
        # get config regex tsel number
        config_tsel_number = get_data_detail(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_SYSTEM_CONFIG'], {"param_key": ALLOW_MSISDN_CONFIG_KEY})
        regex_value = config_tsel_number['param_value']['msisdn']['allow']

        all_data = await get_all_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'redeem_status': 'process'})
        for data in all_data.to_list():
            process_service_id = data['service_id']
            process_channel_trx_id = data['channel_trx_id']

            print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], "Start processing notification!")
            print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Data payload: {data}")
            trx_result = get_data_detail(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_TRX_MASTER'], {'transaction_id': data['tere_trx_id']})
            if trx_result is None:
                print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Transaction not found in transaction_master: {data}")
                continue
            
            if trx_result['status'] == 'Fail':
                update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'transaction_id': data['tere_trx_id']}, {'redeem_status': 'fail', 'notification_status': 'not_sent'})
                print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Transaction failed! Transaction in transaction_master was failed: {data}")
                continue

            if trx_result['status'] == 'Success':
                # check is tsel number
                # if tsel number false, update redeem status to success and notif = not_sent
                if not is_tsel_msisdn(regex_value, data['no_handphone']):
                    print('not tsel msisdn')
                    update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'transaction_id': data['tere_trx_id']}, {'redeem_status': 'success', 'notification_status': 'not_sent'})
                    print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Notification not sent! Non tsel msisdn: {data}")
                    continue

                # get notif template
                keyword_detail = get_data_detail(MONGO_URI, config['MONGO']['DATABASE'], "keywords", {'eligibility.name': data['keyword']}, {'eligibility': 1, 'notification': 1})
                is_notif_sent = False
                for detail in keyword_detail['notification']:
                    if NOTIF_VIA_SMS in detail['via'] and detail['code_identifier'] == REDEEM_SUCCESS_TYPE:
                        print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Send notification via sms to {data['no_handphone']}")
                        notif_template = detail['notification_content']
                        notif_template = notif_template.replace(config['WELCOME_POIN_INDIHOME']['POIN_NOTIF_TEMPLATE'], str(data['point_earned']))
                        await send_sms(log_file, data, notif_template)
                        is_notif_sent = True
                        break
                
                if not is_notif_sent:
                    update_data(MONGO_URI, config['MONGO']['DATABASE'], config['MONGO']['COL_IH_WELCOME_POIN_TASK'], {'transaction_id': data['tere_trx_id']}, {'redeem_status': 'success', 'notification_status': 'not_sent'})
                    print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], f"Notification not sent! Notif template not found: {data}")
                    
                print_trx_to_log_file(log_file, 'notification', data['service_id'], data['channel_trx_id'], "Finish processing notification!")
    except Exception as e:
        print_trx_to_log_file(log_file, 'notification', process_service_id, process_channel_trx_id, f"Error processing data: {e.__class__.__name__} - {e}")

async def main():
    """Send notification"""
    today = datetime.date.today().strftime('%Y%m%d')
    log_file = f'{LOGS}/welcome.poin.indihome.{today}.notif.log'
    await process_all_data(log_file)
    await asyncio.sleep(1)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
