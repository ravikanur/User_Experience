import os, uuid
import logging
from datetime import datetime

def create_log_file():
    LOG_FILE = f"{datetime.now().strftime('%Y_%m_%d')}.log"
    LOG_FILE_DIR_PATH = "./logs"
    os.makedirs(LOG_FILE_DIR_PATH, exist_ok=True)
    LOG_FILE_PATH = os.path.join(LOG_FILE_DIR_PATH, LOG_FILE)
    return LOG_FILE_PATH

class Logger:
    unique_id = uuid.uuid1()

    logging.basicConfig(filename= create_log_file(),
                        format= "[ %(asctime)s ] - %(unique_id)s - %(name)s - %(levelname)s - %(message)s",
                        level=logging.INFO)

    old_f = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = Logger.old_f(*args, **kwargs)
        record.unique_id = Logger.unique_id
        return record

    logging.setLogRecordFactory(record_factory)