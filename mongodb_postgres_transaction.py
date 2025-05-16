import csv
import datetime
import argparse
import pandas
import random
import bson
import os
import psycopg2

from pymongo import MongoClient
from datetime import datetime,timedelta

print("Starting connection...")
print("process start time: " + datetime.today().strftime('%Y-%m-%d %H:%M:%S'))

#Creating a postgresql connection
conn = psycopg2.connect(database="slreport_db",
                        host="10.59.102.135",
                        user="slreport",
                        password="sLREPort182*",
                        port="9989")

#Creating a pymongo connection
client = MongoClient("mongodb://dbApp:Sm!le&TS3L@poinmgttbspdb1,poinmgttbspdb2,poinmgtbsdpdb1,poinmgtbsdpdb2/?authSource=admin")

#Getting the database instance
db = client['SLCoreCustomer']

#Variables
rows = []
count = 0
chunk = 100000
last_create_time = ""

header = ["_id","transaction_no","type","channel","program","serial_no","payment_id","account_no","lacci","member_tier","phone","bill_amount","accum_amount","amount","total_revenue","remark","status","agent","create_time","create_local_time","member_id","realm_id","branch_id","merchant_id"]

#Getting last update
with conn:
     with conn.cursor() as sqlcursor:
          sqlcursor.execute("select max(create_time) from tmp_transaction")
          #print(sqlcursor.rowcount-1)
          row = sqlcursor.fetchone()
          if row[0] is None:
             last_create_time = '2024-12-30 17:00:00.000'
             print("EQ-WIB Create Time: " + last_create_time)
          else:
             last_create_time = row[0].strftime("%Y-%m-%d %H:%M:%S.%f")
             print("EQ-WIB create Time: " + last_create_time)

def origin(x):
   origin_cut = x['origin'].split('.')[0]
   return origin_cut

#Batch Process to Import
def batch(count,rows):
    df = pandas.DataFrame(rows)
    df = df.reindex(columns=header)

    dtm = pandas.to_datetime(df['create_time'], format='%Y-%m-%d %H:%M:%S.%f')
    dtm =  dtm + timedelta(hours=+7)
    df['create_time'] = dtm.dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    df['bill_amount'] = df['bill_amount'].fillna(0).astype(int)
    df['accum_amount'] = df['accum_amount'].fillna(0).astype(int)
    df['amount'] = df['amount'].fillna(0).astype(int)
    df['total_revenue'] = df['total_revenue'].fillna(0).astype(int)
    df['remark'] = df['remark'].fillna('')
    print(df)
    df.to_csv('transaction.tmp',header=False,index=False)
    with open('transaction.tmp') as f:
         next(f)
         with conn:
              with conn.cursor() as sqlcursor:
                   sqlcursor.copy_expert("COPY tmp_transaction FROM STDIN DELIMITER ',' CSV QUOTE '\"'",f)
    print("Processed " + str(count) + " rows")


#Query Row Data to Export

dtm  = pandas.to_datetime(last_create_time,format='%Y-%m-%d %H:%M:%S.%f')
dtm  = dtm + timedelta(hours=-7)

dtm_str = dtm.strftime('%Y-%m-%d %H') + ':00:00.000'
dtm_0 = pandas.to_datetime(dtm_str,format='%Y-%m-%d %H:%M:%S.%f')

date_start = dtm
date_end   = dtm_0 + timedelta(hours=+2)

print("GT-UTC Create Time: " +  date_start.strftime('%Y-%m-%d %H:%M:%S.%f'))
print("LT-UTC Create Time: " +  date_end.strftime('%Y-%m-%d %H:%M:%S.%f'))

query_m = {
   "create_time": {"$gt": date_start, "$lt" : date_end}
}

output_m = {
   "_id":1,
   "transaction_no":1,
   "type":1,
   "channel":1,
   "program":1,
   "serial_no":1,
   "payment_id":1,
   "account_no":1,
   "lacci":1,
   "member_tier":1,
   "phone":1,
   "bill_amount":1,
   "accum_amount":1,
   "amount":1,
   "total_revenue":1,
   "remark":1,
   "status":1,
   "agent":1,
   "create_time":1,
   "create_local_time":1,
   "member_id":1,
   "realm_id":1,
   "branch_id":1,
   "merchant_id":1
}

counts = db.transaction.count_documents(query_m)
print("Total Documents: " + str(counts))

cursor = db.transaction.find(query_m,output_m)

for row in cursor:
    if count % chunk == 0 and len(rows) > 0:
       batch(count,rows)
       rows = []
    #print(row)
    rows.append(row)
    count += 1

batch(count,rows)
print("process end time: " + datetime.today().strftime('%Y-%m-%d %H:%M:%S'))

#Close connection
client.close()
print("Closed connection...")
