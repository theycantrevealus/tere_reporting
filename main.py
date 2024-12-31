import schedule
import time
from modules.logging import Logging, LoggingType

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

# Schedule tasks

# Update configuration every seconds
# schedule.every(1).seconds.do(get_configuration)

schedule.every(10).seconds.do(run_process, '0POIN')

# Every day on 06:00 AM Generate 0POIN
# schedule.every().days.on("06:00").do(run_process, '0POIN')

while True:
    schedule.run_pending()
    time.sleep(1)