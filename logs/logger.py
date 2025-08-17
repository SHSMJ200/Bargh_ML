import os
import logging
from logging.handlers import RotatingFileHandler
import coloredlogs


class CustomLogger:
    def __init__(self, name: str, log_file_name: str):
        self.logger = logging.getLogger(name=name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

        current_dir = os.path.dirname(os.path.abspath(__file__))
        log_file_path = current_dir + log_file_name

        if not self.logger.handlers:
            file_handler = RotatingFileHandler(log_file_path, mode='a', maxBytes=5 * 1024 * 1024, backupCount=3,
                                               encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)

            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            file_formatter = logging.Formatter(
                "%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s"
            )

            console_formatter = coloredlogs.ColoredFormatter(
                fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

            file_handler.setFormatter(file_formatter)
            console_handler.setFormatter(console_formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def get_logger(self):
        return LoggerAdapter(self.logger)


class LoggerAdapter(logging.LoggerAdapter):

    def debug(self, msg: str, *args, **kwargs):
        super().debug(msg, *args, **{**kwargs, "stacklevel": 2})

    def info(self, msg: str, *args, **kwargs):
        super().info(msg, *args, **{**kwargs, "stacklevel": 2})

    def warning(self, msg: str, *args, **kwargs):
        super().warning(msg, *args, **{**kwargs, "stacklevel": 2})

    def error(self, msg: str, *args, **kwargs):
        super().error(msg, *args, **{**kwargs, "stacklevel": 2})

    def critical(self, msg: str, *args, **kwargs):
        super().critical(msg, *args, **{**kwargs, "stacklevel": 2})
