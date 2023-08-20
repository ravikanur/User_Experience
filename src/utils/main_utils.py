import os, sys
import yaml

from src.exception import UserException
from src.logger import logging

def read_yaml_file(path: str) -> dict:
    try:
        logging.info("Entering read_yaml_file function")
        with open(path, 'rb') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.error(e)
        raise UserException(e,sys) from e

def write_yaml_file(path: str, filename: str):
    try:
        logging.info("Entering write_yaml_file function")
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'w') as file:
            yaml.dump(filename, file)
    except Exception as e:
        logging.error(e)
        raise UserException(e, sys) from e