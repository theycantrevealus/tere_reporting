"""Load"""
import subprocess
import configparser
from datetime import datetime
from dateutil import (parser, tz)
import pandas as pd
from pymongo import MongoClient

def write_to_result(content = "", target="apps.log"):
    """Load"""
    with open(target, "a", encoding="UTF-8") as log_file:
        log_file.write(f"{content}\n")
        log_file.flush()
        
def main():
    """Load"""
    print("Source File: ", end = "")
    # target_source = input()
    target_source = "revoke/testing_cache.csv"
    print("")

    print("Log File: ", end = "")
    # target_log = input()
    target_log = "logs/testing_cache.log"
    print("")

    print("Core transaction from (YYYY-MM-DDTHH:II:SSZ): ", end = "")
    # parse_date = input()
    parse_date = "2024-12-01T17:00:00Z"
    print("")


    config = configparser.ConfigParser()
    config.read('.env')
    
    ENVIRONMENT = config['ENVIRONMENT']['TARGET']
    if ENVIRONMENT == 'development':
        MONGO_URI = "mongodb://" + config['MONGO']['HOST'] + "/"
    else:
        MONGO_URI = "mongodb://" + config['MONGO']['USERNAME'] + ":" + urllib.parse.quote_plus(config['MONGO']['PASSWORD']) + "@" + config['MONGO']['HOST'] + "/?" + config['MONGO']['EXTRA']

    client = MongoClient(MONGO_URI)
    database = client.get_database(config['MONGO']['DATABASE_CORE'])
    collection = database.get_collection(f"{config['MONGO']['COL_TRANSACTION']}")
    check_db = pd.DataFrame(list(collection.aggregate([
        {
            "$match": {
                "remark": "Koreksi Poin|ADJPOIN2025|Others",
                "create_time": {
                    "$gte": parser.isoparse(f'{parse_date}')
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
    report_database = client.get_database(config['MONGO']['DATABASE_CORE'])
    report_collection = report_database.get_collection(f"{config['MONGO']['COL_TRANSACTION']}")
    check_db = pd.DataFrame(list(report_collection.aggregate([
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
    # report_df = report_df.map(lambda x: x.replace('\n', '') if isinstance(x, str) else x)
    report_df = report_df.applymap(lambda x: x.replace('\n', '') if type(x) == str else x)

    snapshot_target = "snapshot/revoked.log"
    # write_to_result(content="", target=snapshot_target)
    write_to_result(content="========================================================================", target=snapshot_target)
    write_to_result(content=f"    Source               : {target_source}", target=snapshot_target)
    write_to_result(content=f"    Log                  : {target_log}", target=snapshot_target)
    write_to_result(content=f"    DB Transaction From  : {parse_date}", target=snapshot_target)
    write_to_result(content="+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+", target=snapshot_target)
    write_to_result(content="| [LOG - Success] | [LOG - Success] | [LOG - Success] | [LOG - Fail] | [LOG - Fail] | [LOG - Fail] | [LOG - Partial] | [LOG - Partial] | [LOG - Partial] | [Source] | [Source] | [DB]         | [DB]         |", target=snapshot_target)
    write_to_result(content="| Row             | Total Redeem    | Revoke          | Row          | Total Redeem | Revoke       | Row             | Total Redeem    | Revoke          | Row      | Revoke   | Row          | Total Redeem |", target=snapshot_target)
    write_to_result(content="+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+", target=snapshot_target)
    for row in report_df.itertuples(index=False):
        write_to_result(content=f"| File Name : {row.file:191}|", target=snapshot_target)
        write_to_result(content="+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+", target=snapshot_target)
        write_to_result(content=f"| {row.success_row:15} | {row.success_total_redeem:15} | {row.success_revoke:15} | {row.fail_row:12} | {row.fail_total_redeem:12} | {row.fail_revoke:12} | {row.partial_row:15} | {row.partial_total_redeem:15} | {row.partial_revoke:15} | {row.source_row:8} | {row.source_revoke:8} | {row.db_row:12} | {row.db_total_redeem:12} |", target=snapshot_target)
        write_to_result(content="+-----------------+-----------------+-----------------+--------------+--------------+--------------+-----------------+-----------------+-----------------+----------+----------+--------------+--------------+", target=snapshot_target)
    write_to_result(content="", target=snapshot_target)
    write_to_result(content="", target=snapshot_target)
    client.close()


if __name__ == "__main__":
    """Load"""
    main()