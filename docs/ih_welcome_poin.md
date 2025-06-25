## Indihome Welcome POIN - Quick Win 1 - Proposal

### Skenario:
Persiapan Analisa Skenario Technical Welcome POIN IH
- ✅ Semua response e2e redeem harus ada di log
- ✅ Semua response e2e notifikasi ke smsgw harus ada di log
- ✅ Proses harus tercatat di collection `ih_welcome_poin_tasks` agar memastikan persistensi proses
- ✅ Tracing log / status proses pada collection dapat dicari berdasarkan : transaction_id tere, transaction_id pada file (channel), service_number, no_handphone pada file untuk mengetahui:
	- ✅ Status redeem
	- ✅ Status notifikasi
	- ✅ Keyword terkait sesuai dengan systemconfig sesuai dengan fee apakah menggunakan config keyword yang sesuai atau tidak

---

### Collection for log jobs & send SMS notification
Collection name: `ih_welcome_poin_tasks`
```json
[
  {
    "source_file": "welcomepoin_IH_20250624.txt",
    "channel_trx_id": "AOi42506241133393396bc6a0", # channel order_id
    "tere_trx_id": "TRX_20241015165900", # tere trx id
    "service_id": "111144111105",
    "fee": 166500,
    "keyword": "WELCOMEPOIN",
    "process_state": "COMPLETED",
    "no_handphone": "6283456789012",
    "secondary_phone": null,
    "redeem_status": "process", # process, completed, not_process (if process_state != "COMPLETED")
    "notification_status": "pending", # pending, sent, not_sent
    "process_at": "2024-10-15T16:59:00.000Z"
  },
  {
    "source_file": "welcomepoin_IH_20250624.txt",
    "channel_trx_id": "AOk425062408471205330dc60", # channel order_id
    "tere_trx_id": "TRX_20241015165906", # tere trx id
    "service_id": "111144111106",
    "fee": 50000,
    "keyword": "WELCOMEPOIN",
    "process_state": "COMPLETED",
    "no_handphone": "6283456789013",
    "secondary_phone": null,
    "redeem_status": "completed", # process, completed, not_process (if process_state != "COMPLETED")
    "notification_status": "sent", # pending, sent, not_sent
    "process_at": "2024-10-15T16:59:00.000Z"
  }
]
```

#### Enum Parameter:
- redeem_status: process, completed, not_process (if process_state in file != "COMPLETED")
- notification_status: pending, sent, not_sent

---

### System config for range fee and poin
```json
{
  "_id": "685bc3f4e2723ad4d3a0e009",
  "param_key": "INDIHOME_WELCOME_POIN_FEE_RANGE",
  "description": "",
  "param_value": {
    "static": {
      "50000": 500,
      "55000": 500,
      "150000": 1500,
      "166500": 1500
    },
    "ranged": [
      {
        "min_amount": 1000,
        "max_amount": 55000,
        "poin": 500
      },
      {
        "min_amount": 61000,
        "max_amount": 100000,
        "poin": 1000
      },
      {
        "min_amount": 110000,
        "max_amount": 170000,
        "poin": 1500
      }
    ],
    "divide_by_fee": {
      "divide_value": 100
    }
  }
}
```

#### Config Option:
- static: for static fee and poin
- ranged: for ranged fee and poin
- divide_by_fee: for divide fee by value

---

### Example log to file
Log file naming: `ih_welcome_poin.log`
```log
log_date | activity | service_id | channel_trx_id | message
2025-06-24 16:15:49 | redeem | 111401230542 | AOi42506241133393396bc6a0 | Hit redeem API using payload: "{"msisdn": "111401230542", "channel_id": "channel_id", "keyword": "keyword", "send_notification": true}" 
2025-06-24 16:15:49 | redeem | 111401230542 | AOi42506241133393396bc6a0 | Redeem has processed using payload: "{"msisdn": "111401230542", "channel_id": "channel_id", "keyword": "keyword", "send_notification": true}"
2025-06-24 16:15:49 | redeem | 111401230542 | AOi42506241133393396bc6a0 | Notification has sent                                                                                             
2025-06-24 16:15:49 | redeem | 111402180956 | AOi4250624022827472306030 | Hit redeem API using payload: "{"msisdn": "111401230542", "channel_id": "channel_id", "keyword": "keyword", "send_notification": true}" 
2025-06-24 16:15:49 | redeem | 111402180956 | AOi4250624022827472306030 | Redeem has processed using payload: "{"msisdn": "111401230542", "channel_id": "channel_id", "keyword": "keyword", "send_notification": true}" 
2025-06-24 16:15:49 | redeem | 111402180956 | AOi4250624022827472306030 | Notification has sent
2025-06-24 16:15:49 | redeem | 111516106271 | AOi42506241133393396bc6a0 | Redeem is not processed. process_state != "COMPLETED"
```

#### NB: Make log is useful, can story telling what transaction do. Like log in deploy.sh in devsecops activity