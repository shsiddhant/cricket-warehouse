import logging
from logging.handlers import MemoryHandler, RotatingFileHandler
from cricketwarehouse import LOG_FILE


def custom_logger(name: str, maxBytes: int = 5242880, backupCount: int = 2):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    log_filehandler = RotatingFileHandler(
        LOG_FILE, maxBytes=maxBytes, backupCount=backupCount
    )
    log_filehandler.setLevel(logging.INFO)
    log_filehandler.setFormatter(log_format)

    log_consolehandler = logging.StreamHandler()
    log_consolehandler.setLevel(logging.ERROR)
    log_consolehandler.setFormatter(log_format)

    log_batchhandler = MemoryHandler(
        capacity=1000, flushLevel=logging.ERROR, target=log_filehandler)

    logger.addHandler(log_consolehandler)
    logger.addHandler(log_batchhandler)

    return logger
