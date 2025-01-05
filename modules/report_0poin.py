""" What should I said ??? """
from modules.logger import Logger, LoggerFileHandler

class Report0POIN:
    """ What should I said ??? """
    def __init__(self):
        self.__log = Logger(LoggerFileHandler("info.log", "warning.log", "debug.log", "error.log", "exception.log"))

    def try_run(self):
        """ What should I said ??? """
        self.__log.info("info LOG")
        self.__log.debug("debug LOG")
        self.__log.error("error LOG")
        self.__log.warning("warning LOG")
        self.__log.exception("exception LOG")