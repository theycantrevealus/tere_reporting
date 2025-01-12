""" What should I said ??? """
import os
import subprocess
from datetime import *
from dateutil import parser
import pymongo
from modules.mongo import Mongo, QueryType
from modules.logger import Logger, LoggerFileHandler

class Report0POIN:
    """ What should I said ??? """
    def __init__(self):
        self.__log = Logger(LoggerFileHandler("info.log", "warning.log", "debug.log", "error.log", "exception.log"))
        try:
            self.mongo = Mongo('SLRevamp2', 'transaction_master')
        except pymongo.errors.ConnectionFailure as e:
            self.__log.exception(f'{e}')

    def convert_datetime(self, dt_str: str):
        """ What should I said ??? """
        return parser.isoparse(dt_str).astimezone()
    
    def formatted_trx_date(self, dt_str):
        """ What should I said ??? """
        return datetime.strptime(f'{dt_str}'.split("+")[0], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')
    
    def allowed_msisdn(self, msisdn):
        """ What should I said ??? """
        prefixes = ("08", "62", "81", "82", "83", "85", "628")
        return any(msisdn.startswith(prefix) and msisdn[len(prefix):].isdigit() for prefix in prefixes)
    
    def allowed_indihome_number(self, msisdn):
        """ What should I said ??? """
        return self.allowed_msisdn(msisdn) is False
    
    def format_msisdn_to_id(self, msisdn: str) -> str:
        """ What should I said ??? """
        if msisdn:
            msisdn_str = f'{msisdn}'
            return msisdn_str.replace('08', '628', 1).replace('8', '628', 1) if msisdn_str.startswith(('08', '8')) else msisdn_str
        return msisdn
    
    def format_indihome_number_to_non_core(self, cust_number):
        """ What should I said ??? """
        return '1' + cust_number[2:] if cust_number and cust_number.startswith('01') else cust_number

    def format_file_name(self, dt_str):
        """ What should I said ??? """
        return datetime.strptime(f'{dt_str}'.split("+")[0], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d')
    
    def msisdn_combine_format_to_id(self, msisdn) -> str:
        """ What should I said ??? """
        if self.allowed_msisdn(msisdn):
            return self.format_msisdn_to_id(msisdn)
        elif self.allowed_indihome_number(msisdn):
            return self.format_indihome_number_to_non_core(msisdn)
        else:
            return ""

    def produce_data(self, start_date, end_date, extra = ""):
        """ What should I said ??? """
        process_start_time = datetime.now()
        self.__log.info(f"Process start at {process_start_time} [{extra}]")

        pipeline = [
            {
                "$match": {
                    "keyword": { "$in": [ "0POIN" ] },
                    "transaction_date": {
                        "$gte": start_date,
                        "$lt": end_date,
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "keyword": "$keyword",
                        "msisdn": "$msisdn"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "keyword": "$_id.keyword",
                    "msisdn": "$_id.msisdn",
                    "isindihome": {
                        "$cond": {
                            "if": {
                                "$regexMatch": {
                                    "input": "$_id.msisdn",
                                    "regex": "^(08|62|81|82|83|85|628)+[0-9]+$"
                                }
                            },
                            "then": "false",
                            "else": "true"
                        }
                    }
                }
            }
        ]

        projection = {
            "_id": 0,
            "msisdn": 1,
            "keyword": 1
        }

        self.__log.info("Fetching data")

        filename = f"dci_{self.format_file_name(end_date)}.dat"
        target_file_name = f"report/0POIN/{filename}"

        with open(target_file_name, "a", encoding='utf-8') as file_writer:
            file_writer.write("MSISDN|KEYWORD|ISINDIHOMENUMBER\n")
            for batch in self.mongo.batch_read(pipeline, projection, QueryType.AGGREGATE):
                fields = batch.columns.tolist()
                batch_numpy = batch.to_numpy()
                for line in batch_numpy:
                    to_write = (
                        f'{line[fields.index("msisdn")]}|'
                        f'{line[fields.index("keyword")]}|'
                        f'{line[fields.index("isindihome")]}'
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
        self.__log.info(f'{"Control file".ljust(20, " ")}: {ctl_cat.stdout}', tabular_result_tab)

        linecount = subprocess.run(["wc", "-l", target_file_name], capture_output=True, text=True, check=True)
        self.__log.info( f'{"Line Count".ljust(20, " ")}: {linecount.stdout}', tabular_result_tab)

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
        self.__log.info(f'{"Execution time".ljust(20, " ")}: {(datetime.now() - process_start_time)}', tabular_result_tab)

    # def try_run(self):
    #     """ What should I said ??? """
    #     self.__log.info("info LOG")
    #     self.__log.debug("debug LOG")
    #     self.__log.error("error LOG")
    #     self.__log.warning("warning LOG")
    #     self.__log.exception("exception LOG")