import schedule
import time
from modules.logging import Logging, LoggingType
from modules.file import File
from modules.mongo import Mongo

Logging = Logging()



def get_configuration():
    print(f'Get Configuration')

def run_process(process: str):
    Logging.log(LoggingType.INFO, f'Running {process} process')

def finish_process(process: str):
    Logging.log(LoggingType.SUCCESS, f'{process} generated successfully')

def sample_logging():
    Logging.log(LoggingType.INFO, 'This is sample of info logging')
    Logging.log(LoggingType.WARNING, 'This is sample of warning logging')
    Logging.log(LoggingType.SUCCESS, 'This is sample of success logging')
    Logging.log(LoggingType.ERROR, 'This is sample of error logging')
    Logging.log(LoggingType.GENERAL, 'This is sample of general logging')



# Mongo Restore From JSON
def example_mongo_restore():
    restore_cron_configs = Mongo('SLRevamp2', 'cronconfigs')
    restore_cron_configs.restore('file/cronConfigsExp.json')


# Mongo JSON to CSV
def example_json_to_csv():
    json_to_csv = File()
    json_to_csv.mongo_json_to_csv('file/cronConfigsExp.json','file/cron_config_deep_search.csv', [
        'name',
        'description',
        'interval',
        'target_topic',
        'parameter.keyword_name',
        'parameter.keyword',
        'parameter.field_keyword',
        'data.keyword'
    ])





# Schedule tasks
# Update configuration every seconds
schedule.every(1).seconds.do(get_configuration)
# schedule.every(10).seconds.do(run_process, '0POIN')
# schedule.every().days.on("06:00").do(run_process, '0POIN')

while True:
    schedule.run_pending()
    time.sleep(1)