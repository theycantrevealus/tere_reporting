""" What should I said ??? """
import os
import subprocess
import configparser
import urllib.parse
from typing import Iterable
from datetime import datetime
import psycopg2

import pandas as pd
from dateutil import parser

def convert_datetime(dt_str: str):
    """ What should I said ??? """
    return parser.isoparse(dt_str).astimezone()

def formatted_trx_date(dt_str, format_date):
    """ What should I said ??? """
    return datetime.strptime(f'{dt_str}'.split("+", maxsplit=1)[0], '%Y-%m-%d %H:%M:%S').strftime(format_date)
    # return datetime.strptime(f'{dt_str}'.split("+", maxsplit=1)[0], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')

def allowed_msisdn(msisdn):
    """ What should I said ??? """
    prefixes = ("08", "62", "81", "82", "83", "85", "628")
    return any(msisdn.startswith(prefix) and msisdn[len(prefix):].isdigit() for prefix in prefixes)

def allowed_indihome_number(msisdn):
    """ What should I said ??? """
    return allowed_msisdn(msisdn) is False

def format_msisdn_to_id(msisdn: str) -> str:
    """ What should I said ??? """
    if msisdn:
        msisdn_str = f'{msisdn}'
        return msisdn_str.replace('08', '628', 1).replace('8', '628', 1) if msisdn_str.startswith(('08', '8')) else msisdn_str
    return msisdn

def format_indihome_number_to_non_core(cust_number):
    """ What should I said ??? """
    return '1' + cust_number[2:] if cust_number and cust_number.startswith('01') else cust_number

def msisdn_combine_format_to_id(msisdn) -> str:
    """ What should I said ??? """
    if allowed_msisdn(msisdn):
        return format_msisdn_to_id(msisdn)
    elif allowed_indihome_number(msisdn):
        return format_indihome_number_to_non_core(msisdn)
    else:
        return ""

def validation_keyword_point_value_rule(payload, total_point=None) -> str:
    """ What should I said ??? """
    if isinstance(payload, dict):
        eligibility = payload.get('keyword', {}).get('eligibility')
        result = 0

        if eligibility:
            if total_point is not None:
                result = total_point
            elif 'total_redeem' in payload.get('incoming', {}):
                result = payload['incoming']['total_redeem']

            point_value = eligibility.get('point_value')  # Corrected key
            if point_value == 'Fixed':
                result = eligibility['point_redeemed']
            elif point_value == 'Flexible':
                if result <= 0:
                    result = eligibility['point_redeemed']
            elif point_value == 'Fixed Multiple':
                if result > 0:
                    result = eligibility['point_redeemed']
        return result
    else:
        return ""

def postgre_batch_read(
        cursor,
        query,
        date_from,
        date_to,
        batch_size: int = 100
) -> Iterable[pd.DataFrame]:
    """ What should I said ??? """
    # offset = 0
    # query_parse = query % (date_from, date_to, offset, batch_size)
    query_parse = query % (date_from, date_to)
    print(f"Query : {query_parse}")
    print("=================================")
    print(f"Executing query...(BATCH:{batch_size})")
    cursor.execute(query_parse)
    data = cursor.fetchall()
    print("=================================")
    print("DONE !!")
    print("=================================")
    # batch = list(itertools.islice(data, batch_size))
    yield pd.DataFrame(data)

# =========================================================================================================================================================================


print("=== ATP MANUAL GENERATOR ===")
print("")

# Variable
# =========================================================================================================================================================================
print("Date (YYYY-MM-DD): ", end = "")
PARSE_DATE = str(input())
print("")

date_obj = pd.to_datetime(PARSE_DATE)
last_day = date_obj - pd.Timedelta(days=1)

# Example : "2024-10-14T17:00:00.000Z 2024-10-15T16:59:00.000Z"

print("File name: ", end = "")
FILEFORMAT = "dat"
FILENAME = f"fact_atp_redeem_{formatted_trx_date('', '%Y%m%d')}.{FILEFORMAT}"
SINGLE_FILENAME = FILENAME
print("")

from_date = parser.isoparse(f'{last_day.strftime("%Y-%m-%d")}T17:00:00.000Z')
to_date = parser.isoparse(f'{PARSE_DATE}T17:00:00.000Z')
print(f"GENERATING...[{from_date}] - [{to_date}]")
print("")
print("")


config = configparser.ConfigParser()
config.read('.env')

ENVIRONMENT = config['ENVIRONMENT']['TARGET']
if ENVIRONMENT == 'development':
    MONGO_URI = "mongodb://" + config['MONGO']['HOST'] + "/"
else:
    MONGO_URI = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']

TARGET_DIR = config['RESULT']['DIR']

BATCH_SIZE_PROCESS = int(config['CONFIG']['BATCH_SIZE'])

FILENAME = TARGET_DIR + "/ATP/" + FILENAME

process_start_time = datetime.now()
# =========================================================================================================================================================================

try:
    postgre_conn = psycopg2.connect(database="slreport_db",
                        host="10.59.102.135",
                        user="slreport",
                        password="sLREPort182*",
                        port="9989",
                        options="-c search_path=mongo")

    QUERY = "SELECT " \
    "transaction_id," \
    "keyword, " \
    "keyword_title," \
    "execution_type," \
    "product_id," \
    "period1," \
    "period2," \
    "subscriber_id," \
    "msisdn," \
    "return_value," \
    "execution_date," \
    "channel_code," \
    "transaction_status," \
    "trdm_last_act," \
    "trdm_act_status," \
    "trdm_evd_id," \
    "trdm_flag_kirim," \
    "trdm_geneva_exec," \
    "trdm_keyword," \
    "trdm_tgl_kirim," \
    "channel_transaction_id," \
    "card_type," \
    "brand," \
    "subscriber_region," \
    "subscriber_branch," \
    "lacci " \
    "FROM " \
    "report_redeem_transaction " \
    "WHERE " \
    "transaction_date >= '%s' " \
    "AND transaction_date < '%s' "

    with open(FILENAME, "a", encoding='utf-8') as txt_file:
        # txt_file.write("MSISDN|KEYWORD|ISINDIHOMENUMBER\n")
        with postgre_conn:
            with postgre_conn.cursor() as sqlcursor:
                for batch in postgre_batch_read(sqlcursor, QUERY, from_date, to_date, BATCH_SIZE_PROCESS):
                    fields = batch.columns.tolist()
                    batch_numpy = batch.to_numpy()
                    print(batch_numpy)
                    print("=================================")
                    for line in batch_numpy:
                        TO_WRITE = (
                            f'{line[0]}|'
                            f'{line[1]}|'
                            f'{line[2]}'
                        )
                        txt_file.write(TO_WRITE + "\n")
                        txt_file.flush()
            sqlcursor.close()
        postgre_conn.close()


    # Write CTL file
    with open(FILENAME, "rb") as f:
        rowCount = sum(1 for _ in f)

    fileSize = os.path.getsize(FILENAME)
    ctlName = FILENAME.replace(f".{FILEFORMAT}", ".ctl")
    with open(ctlName, "w", encoding='utf-8') as ctl_file:
        ctl_file.write(f'{SINGLE_FILENAME}|{rowCount}|{fileSize}')

    print("===================== LINE COUNT ========================")
    subprocess.run(["wc", "-l", FILENAME], check=True)
    print("")
    print("")
    print("===================== SAMPLE RESULT =====================")
    subprocess.run(["head", FILENAME], check=True)
    if (rowCount > 10):
        print("...")
        print("...")
        print("...")
        subprocess.run(["tail", "-10", FILENAME], check=True)
    print("")
    print("")
    print("===================== CTL RESULT =====================")
    subprocess.run(["cat", ctlName], check=True)
    print("")
    print("")
    print("=========================================================")
    print(f"--- DONE at [{(datetime.now() - process_start_time)}] seconds ---")
    print("")
    print("")
except psycopg2.Error as postgreError:

    print(f"Unable to find the document due to the following error: {postgreError}")
