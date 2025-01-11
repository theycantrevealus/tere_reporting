""" What should I said ??? """
import os
import subprocess
from datetime import datetime
from dateutil import parser
import pymongo
import pandas as pd
from modules.mongo import Mongo, QueryType
from modules.logger import Logger, LoggerFileHandler
# from tabulate import tabulate

class ReportFactDetail:
    """ What should I said ??? """
    def __init__(self):
        self.__log = Logger(LoggerFileHandler("info.log", "warning.log", "debug.log", "error.log", "exception.log"))

        # Create connection
        try:
            self.mongo = Mongo('SLRevamp2', 'transaction_master')
        except pymongo.errors.ConnectionFailure as e:
            self.__log.exception(f'{e}')

    def convert_datetime(self, dt_str: str):
        """ Convert given time with timezone """
        return parser.isoparse(dt_str).astimezone()

    def format_file_name(self, dt_str):
        """ What should I said ??? """
        return datetime.strptime(f'{dt_str}'.split("+")[0], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d')

    def formatted_trx_date(self, dt_str):
        """ Format given time as required by BI FACT DETAIL """
        dt_obj = pd.to_datetime(str(dt_str).split("+", maxsplit=1)[0], format='%Y-%m-%d %H:%M:%S')
        dt_obj += pd.Timedelta(hours=7)
        return dt_obj.strftime('%d/%m/%Y %H:%M')

    def allowed_msisdn(self, msisdn):
        """ Check given string format is valid msisdn or not """
        prefixes = ("08", "62", "81", "82", "83", "85", "628")
        return any(msisdn.startswith(prefix) and msisdn[len(prefix):].isdigit() for prefix in prefixes)

    def allowed_indihome_number(self, msisdn):
        """ What should I said ??? """
        return self.allowed_msisdn(msisdn) is False

    def validation_keyword_point_value_rule(self, payload, total_point=None) -> str:
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

    def index_finder(self, parent, arr, target, default=None):
        """ Index finder with null handler """
        try:
            return parent[arr.index(target)]
        except ValueError:
            return default

    def produce_data(self, start_date, end_date, extra = ""):
        """ Query transaction_master joining transaction_master_detail """

        process_start_time = datetime.now()
        self.__log.info(f"Process start at {process_start_time} [{extra}]")
        pipeline = [
            {
                "$match": {
                    "transaction_date": {
                        "$gte": start_date,
                        "$lt": end_date,
                    },
                    "status": 'Success',
                    "origin": {
                        "$regex": "^redeem"
                    }
                }
            },
            {
                "$lookup": {
                    "from": "transaction_master_detail",
                    "localField": "transaction_id",
                    "foreignField": "payload.redeem.master_id",
                    "as": "transaction_detail",
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "transaction_date": "$transaction_date",
                    "msisdn": "$msisdn",
                    "keyword": "$keyword",
                    "program_name": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.program"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.program.name",
                                    0
                                ]
                            }
                        }
                    },
                    "program_owner": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.program"
                                    },
                                    0
                                ]
                            },
                            "then": '',
                            "else": {
                                "$convert": {
                                    "input": {
                                        "$arrayElemAt": [
                                            "$transaction_detail.payload.program.program_owner",
                                            0,
                                        ]
                                    },
                                    "to": 'objectId',
                                    "onError": 'null',
                                }
                            }
                        }
                    },
                    "detail_program_owner": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.program"
                                    },
                                    0
                                ]
                            },
                            "then": '',
                            "else": {
                                "$convert": {
                                    "input": {
                                        "$arrayElemAt": [
                                            "$transaction_detail.payload.program.program_owner_detail",
                                            0,
                                        ]
                                    },
                                    "to": "objectId",
                                    "onError": "null"
                                }
                            }
                        }
                    },
                    "created_by": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$ifNull": [
                                            "$created_by", "null"
                                        ]
                                    },
                                    "null"
                                ]
                            },
                            "then": '',
                            "else": {
                                "$convert": {
                                    "input": "$created_by",
                                    "to": "objectId",
                                    "onError": "null",
                                }
                            }
                        }
                    },
                    "lifestyle": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.keyword.eligibility",
                                    },
                                    0
                                ]
                            },
                            "then": '',
                            "else": {
                                "$convert": {
                                    "input": {
                                        "$arrayElemAt": [
                                            {
                                                "$arrayElemAt": [
                                                    "$transaction_detail.payload.keyword.eligibility.program_experience",
                                                    0,
                                                ],
                                            },
                                            0
                                        ]
                                    },
                                    "to": "objectId",
                                    "onError": "null",
                                }
                            }
                        }
                    },
                    "keyword_title": "$keyword",
                    "SMS": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.incoming"
                                    },
                                    0
                                ]
                            },
                            "then": '0',
                            "else": {
                                "$cond": {
                                    "if": {
                                        "$regexMatch": {
                                            "input": {
                                                "$toLower": {
                                                    "$arrayElemAt": [
                                                        "$transaction_detail.payload.incoming.channel_id",
                                                        0,
                                                    ]
                                                }
                                            },
                                            "regex": "sms",
                                        }
                                    },
                                    "then": "1",
                                    "else": "0",
                                }
                            }
                        }
                    },
                    "UMB": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.incoming"
                                    },
                                    0
                                ]
                            },
                            "then": '0',
                            "else": {
                                "$cond": {
                                    "if": {
                                        "$regexMatch": {
                                            "input": {
                                                "$toLower": {
                                                    "$arrayElemAt": [
                                                        "$transaction_detail.payload.incoming.channel_id",
                                                        0,
                                                    ]
                                                }
                                            },
                                            "regex": "umb",
                                        }
                                    },
                                    "then": "1",
                                    "else": "0",
                                }
                            }
                        }
                    },
                    "point": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload",
                                    0
                                ]
                            }
                        }
                    },
                    "subscriber_brand": {
                        "$arrayElemAt": [
                            "$transaction_detail.payload.customer.brand",
                            0
                        ]
                    },
                    "program_regional": {
                        "$substr": [
                            "$msisdn",
                            2,
                            6
                        ]
                    },
                    "cust_value": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.keyword"
                                    },
                                    0
                                ],
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.keyword.eligibility.customer_value",
                                    0,
                                ]
                            }
                        }
                    },
                    "start_date": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.keyword"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.keyword.eligibility.start_period",
                                    0,
                                ]
                            }
                        }
                    },
                    "end_date": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.keyword"
                                    },
                                    0
                                ],
                            },
                            "then": '',
                            "else": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.keyword.eligibility.end_period",
                                    0,
                                ]
                            }
                        }
                    },
                    "merchant_name": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.keyword"
                                    },
                                    0
                                ]
                            },
                            "then": '',
                            "else": {
                                "$convert": {
                                    "input": {
                                        "$arrayElemAt": [
                                            "$transaction_detail.payload.keyword.eligibility.merchant",
                                            0,
                                        ],
                                    },
                                    "to": "objectId",
                                    "onError": "null",
                                },
                            },
                        },
                    },
                    "subscriber_region": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.customer"
                                    },
                                    0
                                ],
                            },
                            "then": "OTHERS",
                            "else": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.customer.region",
                                    0,
                                ],
                            },
                        },
                    },
                    "subscriber_branch": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.customer"
                                    },
                                    0
                                ],
                            },
                            "then": "OTHERS",
                            "else": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.customer.city",
                                    0,
                                ],
                            },
                        },
                    },
                    "channel_code": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$ifNull": [
                                            "$channel_id",
                                            "null"
                                        ]
                                    },
                                    "null"
                                ]
                            },
                            "then": "",
                            "else": "$channel_id",
                        },
                    },
                    "subsidy": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$arrayElemAt": [
                                            "$transaction_detail.payload.keyword.eligibility.program_bersubsidi",
                                            0,
                                        ],
                                    },
                                    "true",
                                ],
                            },
                            "then": "Y",
                            "else": "N",
                        },
                    },
                    "subscriber_tier": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {"$size": "$transaction_detail.payload.customer"
                                     },
                                    0
                                ],
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    {
                                        "$arrayElemAt": [
                                            "$transaction_detail.payload.customer.loyalty_tier.name",
                                            0,
                                        ],
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    "voucher_code": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$transaction_detail.payload.payload.voucher.core",
                                    },
                                    0,
                                ],
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$transaction_detail.payload.payload.voucher.core.voucher_code",
                                    0,
                                ],
                            },
                        },
                    },
                },
            },
            {
                "$lookup": {
                    "from": "accounts",
                    "localField": "created_by",
                    "foreignField": "_id",
                    "as": "created_by",
                },
            },
            {
                "$lookup": {
                    "from": "lovs",
                    "localField": "program_owner",
                    "foreignField": "_id",
                    "as": "program_owner",
                },
            },
            {
                "$lookup": {
                    "from": "locations",
                    "localField": "detail_program_owner",
                    "foreignField": "_id",
                    "as": "detail_program_owner",
                },
            },
            {
                "$lookup": {
                    "from": "lovs",
                    "localField": "lifestyle",
                    "foreignField": "_id",
                    "as": "lifestyle",
                },
            },
            {
                "$lookup": {
                    "from": "locationprefixes",
                    "localField": "program_regional",
                    "foreignField": "prefix",
                    "as": "program_regional",
                },
            },
            {
                '$lookup': {
                    "from": "merchantv2",
                    "localField": "merchant_name",
                    "foreignField": "_id",
                    "as": "merchant_name",
                },
            },
            {
                "$project": {
                    "transaction_date": 1,
                    "msisdn": 1,
                    "keyword": 1,
                    "program_name": 1,
                    "program_owner": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$program_owner"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$program_owner.set_value",
                                    0
                                ]
                            },
                        },
                    },
                    "detail_program_owner": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$detail_program_owner"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$detail_program_owner.name",
                                    0
                                ]
                            },
                        },
                    },
                    "created_by": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$created_by"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$created_by.user_name",
                                    0
                                ]
                            },
                        },
                    },
                    "lifestyle": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$lifestyle"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$lifestyle.set_value",
                                    0
                                ]
                            },
                        },
                    },
                    "category": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$lifestyle"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$lifestyle.set_value",
                                    0
                                ]
                            },
                        },
                    },
                    "keyword_title": 1,
                    "SMS": 1,
                    "UMB": 1,
                    "point": 1,
                    "subscriber_brand": 1,
                    "program_regional": {
                        "$toUpper": {
                            "$arrayElemAt": [
                                "$program_regional.area",
                                0
                            ]
                        },
                    },
                    "cust_value": 1,
                    "start_date": 1,
                    "end_date": 1,
                    "merchant_name": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {
                                        "$size": "$merchant_name"
                                    },
                                    0
                                ]
                            },
                            "then": "",
                            "else": {
                                "$arrayElemAt": [
                                    "$merchant_name.merchant_name",
                                    0
                                ]
                            },
                        }
                    },
                    "subscriber_region": 1,
                    "subscriber_branch": 1,
                    "channel_code": 1,
                    "subsidy": 1,
                    "subscriber_tier": 1,
                    "voucher_code": 1
                },
            }
        ]

        self.__log.info("Fetching data")

        filename = f"poin_fact_report_detail_{self.format_file_name(end_date)}.dat"
        target_file_name = f"report/FACT_DETAIL/{filename}"

        with open(target_file_name, "a", encoding='utf-8') as file_writer:
            for batch in self.mongo.batch_read(pipeline, {}, QueryType.AGGREGATE):
                fields = batch.columns.tolist()
                batch_numpy = batch.to_numpy()
                for line in batch_numpy:
                    transaction_date = ""
                    if line[fields.index('transaction_date')]:
                        transaction_date_unformatted = self.convert_datetime(f'{line[fields.index("transaction_date")]}'.replace(' ', 'T').split('.')[0])
                        transaction_date = f'{self.formatted_trx_date(transaction_date_unformatted)}' or ""
                    start_date = ""
                    if line[fields.index('start_date')]:
                        start_date_unformatted = self.convert_datetime(f'{line[fields.index("start_date")]}'.replace(' ', 'T').split('.')[0])
                        start_date = f'{self.formatted_trx_date(start_date_unformatted)}' or ""

                    end_date = ""
                    if line[fields.index('end_date')]:
                        end_date_unformatted = self.convert_datetime(f'{line[fields.index("end_date")]}'.replace(' ', 'T').split('.')[0])
                        end_date = f'{self.formatted_trx_date(end_date_unformatted)}' or ""
                    # self.__log.info(f"Writing {line[fields.index('msisdn')]}")

                    to_write = (
                        f"{transaction_date}|"
                        f"{self.index_finder(line, fields, 'msisdn', '')}|"
                        f"{self.index_finder(line, fields, 'keyword', '')}|"
                        f"{self.index_finder(line, fields, 'program_name', '')}|"
                        f"{self.index_finder(line, fields, 'program_owner', '')}|"
                        f"{self.index_finder(line, fields, 'detail_program_owner', '')}|"
                        f"{self.index_finder(line, fields, 'created_by', '')}|"
                        f"{self.index_finder(line, fields, 'lifestyle', '')}|"
                        f"{self.index_finder(line, fields, 'category', '')}|"
                        f"{self.index_finder(line, fields, 'keyword_title', '')}|"
                        f"{self.index_finder(line, fields, 'SMS', '')}|"
                        f"{self.index_finder(line, fields, 'UMB', '')}|"
                        f"{self.validation_keyword_point_value_rule(line[fields.index('point')]) or ''}|"
                        f"{self.index_finder(line, fields, 'subscriber_brand', '')}|"
                        f"{self.index_finder(line, fields, 'program_regional', '')}|"
                        f"{self.index_finder(line, fields, 'cust_value', '')}|"
                        f"{start_date}|"
                        f"{end_date}|"
                        f"{self.index_finder(line, fields, 'merchant_name', '')}|"
                        f"{self.index_finder(line, fields, 'subscriber_region', '')}|"
                        f"{self.index_finder(line, fields, 'subscriber_branch', '')}|"
                        f"{self.index_finder(line, fields, 'channel_code', '')}|"
                        f"{self.index_finder(line, fields, 'subsidy', '')}|"
                        f"{self.index_finder(line, fields, 'subscriber_tier', '')}|"
                        f"{self.index_finder(line, fields, 'voucher_code', '')}|"
                        f"{self.allowed_indihome_number(line[fields.index("msisdn")])}".lower()
                    )

                file_writer.write(to_write + "\n")
                file_writer.flush()

        self.mongo.client.close()

        self.__log.info("Report write finished")
        self.__log.info("Generating control file")
        extension = filename.rsplit('.', maxsplit=1)[-1]
        with open(target_file_name, "rb") as f:
            row_count = sum(1 for _ in f)

            file_size = os.path.getsize(target_file_name)
            ctl_name = target_file_name.replace(extension, "ctl")
            with open(ctl_name, "w", encoding='utf-8') as ctl_file:
                ctl_file.write(f'{filename}|{row_count}|{file_size}')

        self.__log.info("Control file write finished")


        tabular_result_tab = 25
        self.__log.separator()
        ctl_cat = subprocess.run(["cat", ctl_name], capture_output=True, text=True, check=True)
        self.__log.info(f"{"Control file".ljust(20, " ")}: {ctl_cat.stdout}", tabular_result_tab)

        linecount = subprocess.run(["wc", "-l", target_file_name], capture_output=True, text=True, check=True)
        self.__log.info( f"{"Line Count".ljust(20, " ")}: {linecount.stdout}", tabular_result_tab)

        self.__log.info("Sample Result".ljust(20, " "), tabular_result_tab)

        first_line = subprocess.run(["head", "-10", target_file_name], capture_output=True, text=True, check=True)
        output_f_lines = first_line.stdout.splitlines()
        for fline in output_f_lines:
            self.__log.info(f"${fline}", tabular_result_tab)
        self.__log.info("... <rest of data content> ...", tabular_result_tab)
        last_line = subprocess.run(["tail", "-10", target_file_name], capture_output=True, text=True, check=True)
        output_l_lines = last_line.stdout.splitlines()
        for lline in output_l_lines:
            self.__log.info(f"${lline}", tabular_result_tab)

        self.__log.separator()
        self.__log.info(f"{"Execution time".ljust(20, " ")}: {(datetime.now() - process_start_time)}", tabular_result_tab)
