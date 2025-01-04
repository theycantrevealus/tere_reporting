""" File Logger Module """
import logging

class Logger:
    """ File Logger Module """
    def __init__(self):
        self.logger = logging.getLogger('OPS')
        self.logger.handlers = [h for h in self.logger.handlers if not isinstance(h, logging.StreamHandler)]
        self.logger.setLevel(logging.INFO)
        # logging.basicConfig(
        #     level=logging.INFO,
        #     format='%(asctime)s %(levelname)s: %(message)s',
        #     datefmt='%Y-%m-%d %H:%M:%S'
        # )

        # Handling format
        info_handler = logging.FileHandler('logs/info.log')
        info_formatter = logging.Formatter('[%(asctime)s] - %(levelname)s :: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(info_formatter)
        self.logger.addHandler(info_handler)

        error_handler = logging.FileHandler('logs/error.log')
        error_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(error_formatter)
        self.logger.addHandler(error_handler)

        exception_handler = logging.FileHandler('logs/exception.log')
        exception_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        exception_handler.setLevel(logging.ERROR)
        exception_handler.setFormatter(exception_formatter)
        self.logger.addHandler(exception_handler)

    def info(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.info(f'{"".ljust(wording_tab, " ")} {wording}')

    def warning(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.warning(f'{"".ljust(wording_tab, " ")} {wording}')

    def debug(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.debug(f'{"".ljust(wording_tab, " ")} {wording}')

    def error(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.error(f'{"".ljust(wording_tab, " ")} {wording}')

    def exception(self, wording, wording_tab = 0):
        """ Write to log """
        self.logger.exception(f'{"".ljust(wording_tab, " ")} {wording}')

    def separator(self):
        self.logger.info("========================================================================================================================================================================================================================================================")
