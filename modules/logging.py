from datetime import datetime
from enum import Enum

class LoggingType(str, Enum):
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"
    GENERAL = "GENERAL"

class Logging:
    def __init__(self):
        # Init logging config
        self.log_type_padding = 12

    def log(self, log_type: LoggingType, wording):
        now = datetime.now()
        if log_type == 'INFO':
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[94m[{log_type.center(self.log_type_padding, ' ')}]\033[0m : {wording}')
        elif log_type == 'SUCCESS':
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[92m[{log_type.center(self.log_type_padding, ' ')}]\033[0m : {wording}')
        elif log_type == 'WARNING':
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[93m[{log_type.center(self.log_type_padding, ' ')}]\033[0m : {wording}')
        elif log_type == 'ERROR':
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[91m[{log_type.center(self.log_type_padding, ' ')}]\033[0m : {wording}')
        else:
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - [{log_type.center(self.log_type_padding, ' ')}] : {wording}')

# - Red: \033[91m
# - Green: \033[92m
# - Yellow: \033[93m
# - Blue: \033[94m
# - Reset: \033[0m
# - Closing: \033[0m