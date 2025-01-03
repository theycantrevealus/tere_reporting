from datetime import datetime
from enum import Enum
# from rich import print
# from rich.console import Console
import shutil

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

    def separator(self, log_type: LoggingType):
        self.log(log_type, "========================================================================================================================================================================================================================================================")

    def log(self, log_type: LoggingType, wording, wording_tab = 0):
        now = datetime.now()
        if log_type == 'INFO':
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[94m[{log_type.center(self.log_type_padding, ' ')}]\033[0m :: {"".ljust(wording_tab, " ")} {wording}')
        elif log_type == 'SUCCESS':
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[92m[{log_type.center(self.log_type_padding, ' ')}]\033[0m :: {"".ljust(wording_tab, " ")} {wording}')
        elif log_type == 'WARNING':
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[93m[{log_type.center(self.log_type_padding, ' ')}]\033[0m :: {"".ljust(wording_tab, " ")} {wording}')
        elif log_type == 'ERROR':
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[91m[{log_type.center(self.log_type_padding, ' ')}]\033[0m :: {"".ljust(wording_tab, " ")} {wording}')
        else:
            print(f'[{now.strftime("%Y-%m-%d %H:%M:%S")}] - [{log_type.center(self.log_type_padding, ' ')}] :: {"".ljust(wording_tab, " ")} {wording}')

    # def log_col(self, log_type: LoggingType, wording, column = 1, wording_tab = 0):
        # rows, cols = shutil.get_terminal_size()
        # print(f"Terminal size: ({rows}, {cols})")

        # console = Console()
        # print(f"Cursor position: ({console.height}, {console.width})")

        # col = {
        #     "col_1": f"\033[{rows + 2};1H",
        #     "col_2": f"\033[{rows + 2};20H"
        # }
        #
        # now = datetime.now()
        # if log_type == 'INFO':
        #     print(f'{col[f"col_{str(column)}"]}[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[94m[{log_type.center(self.log_type_padding, ' ')}]\033[0m :: {"".ljust(wording_tab, " ")} {wording}')
        # elif log_type == 'SUCCESS':
        #     print(f'{col[f"col_{str(column)}"]}[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[92m[{log_type.center(self.log_type_padding, ' ')}]\033[0m :: {"".ljust(wording_tab, " ")} {wording}')
        # elif log_type == 'WARNING':
        #     print(f'{col[f"col_{str(column)}"]}[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[93m[{log_type.center(self.log_type_padding, ' ')}]\033[0m :: {"".ljust(wording_tab, " ")} {wording}')
        # elif log_type == 'ERROR':
        #     print(f'{col[f"col_{str(column)}"]}[{now.strftime("%Y-%m-%d %H:%M:%S")}] - \033[91m[{log_type.center(self.log_type_padding, ' ')}]\033[0m :: {"".ljust(wording_tab, " ")} {wording}')
        # else:
        #     print(f'{col[f"col_{str(column)}"]}[{now.strftime("%Y-%m-%d %H:%M:%S")}] - [{log_type.center(self.log_type_padding, ' ')}] :: {"".ljust(wording_tab, " ")} {wording}')

# - Red: \033[91m
# - Green: \033[92m
# - Yellow: \033[93m
# - Blue: \033[94m
# - Reset: \033[0m
# - Closing: \033[0m