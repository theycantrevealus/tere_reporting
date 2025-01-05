""" File Logger Module """
import logging
from enum import Enum
from dataclasses import dataclass

@dataclass
class LoggerFileHandler:
    """ File Logger File Handler """
    info: str = 'info.log'
    warning: str = 'error.log'
    debug: str = 'debug.log'
    error: str = 'error.log'
    exception: str = 'exception.log'

class Logger:
    """ File Logger Module """
    def __init__(self, file_handler: LoggerFileHandler):
        # self.file_handler = LoggerFileHandler("info.log", "warning.log", "debug.log", "error.log", "exception.log")
        self.file_handler = file_handler

        self.logger = logging.getLogger('OPS')
        self.logger.handlers = [h for h in self.logger.handlers if not isinstance(h, logging.StreamHandler)]

    def info(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.handlers.clear()
        info_handler = logging.FileHandler(f'logs/{self.file_handler.info}')
        info_formatter = logging.Formatter('[%(asctime)s] - %(levelname)s :: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(info_formatter)
        self.logger.addHandler(info_handler)
        self.logger.setLevel(logging.INFO)
        self.logger.info("%s", f'{"".ljust(wording_tab, " ")} {wording}')

    def warning(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.handlers.clear()
        warning_handler = logging.FileHandler(f'logs/{self.file_handler.warning}')
        warning_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        warning_handler.setLevel(logging.WARNING)
        warning_handler.setFormatter(warning_formatter)
        self.logger.addHandler(warning_handler)
        self.logger.setLevel(logging.WARNING)
        self.logger.warning("%s", f'{"".ljust(wording_tab, " ")} {wording}')

    def debug(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.handlers.clear()
        debug_handler = logging.FileHandler(f'logs/{self.file_handler.debug}')
        debug_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(debug_formatter)
        self.logger.addHandler(debug_handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("%s", f'{"".ljust(wording_tab, " ")} {wording}')

    def error(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.handlers.clear()
        error_handler = logging.FileHandler(f'logs/{self.file_handler.error}')
        error_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(error_formatter)
        self.logger.addHandler(error_handler)
        self.logger.setLevel(logging.ERROR)
        self.logger.error("%s", f'{"".ljust(wording_tab, " ")} {wording}')

    def exception(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.handlers.clear()
        exception_handler = logging.FileHandler(f'logs/{self.file_handler.exception}')
        exception_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        exception_handler.setLevel(logging.ERROR)
        exception_handler.setFormatter(exception_formatter)
        self.logger.addHandler(exception_handler)
        self.logger.setLevel(logging.ERROR)
        self.logger.exception("%s", f'{"".ljust(wording_tab, " ")} {wording}')

    def separator(self):
        """ Log line separator """
        self.logger.info("========================================================================================================================================================================================================================================================")
