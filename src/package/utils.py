import datetime
import json
import logging
import logging.config
import os
import re
import yaml


def file_to_dict(path: str) -> dict:
    filename: str = os.path.basename(path)

    if filename.endswith('.json'):
        with open(path, 'r') as f:
            return json.load(f)
    elif filename.endswith('.yaml'):
        with open(path, 'r') as f:
            return yaml.load(f, Loader=yaml.FullLoader)


def get_logger_config(path: str, logger_name: str) -> logging.getLogger():
    filename: str = os.path.basename(path)

    if not os.path.exists(path):
        raise FileNotFoundError('logger file is not exist')
    else:
        if filename.endswith('.json'):
            log_config: dict = json.load(open(path))
            logging.config.dictConfig(log_config)
            return logging.getLogger(logger_name)
        elif filename.endswith('.yaml'):
            log_config: dict = yaml.load(open(path), Loader=yaml.FullLoader)
            logging.config.dictConfig(log_config)
            return logging.getLogger(logger_name)


def get_error_location(traceback_msg: str) -> (str, str):
    msg_list: list = [msg.strip() for msg in traceback_msg.split('\n') if not len(msg) == 0]
    line: str = re.search(r'[l][i][n][e][ ]([0-9]{1,})[,]', traceback_msg).group(1)
    method: str = msg_list[-2]

    return line, method


def change_filename_date(filename: str, date: str) -> str:
    date_obj: datetime = datetime.datetime.strptime(date, '%Y%m%d')
    format_is_non_hyphen: object = re.search(r'[0-9]{8}', filename)
    format_is_hyphen: object = re.search(r'[0-9]{4}[-][0-9]{2}[-][0-9]{2}', filename)

    if format_is_non_hyphen is not None:
        date_format: str = date_obj.strftime('%Y%m%d')
        cleaned_filename: str = re.sub(r'[0-9]{8}', date_format, filename)
    elif format_is_hyphen is not None:
        date_format: str = date_obj.strftime('%Y-%m-%d')
        cleaned_filename: str = re.sub(r'[0-9]{4}[-][0-9]{2}[-][0-9]{2}', date_format, filename)
    else:
        raise Exception("the format of a remote filename doesn't contain 'yyyymmdd' or 'yyyy-mm-dd")

    return cleaned_filename


def get_ago_date_list(ago: int = 7) -> list:
    current_date: datetime = datetime.datetime.now()
    date_list: list = []

    for day in range(ago+1, 0, -1):
        date_format: str = (current_date - datetime.timedelta(days=day)).strftime('%Y%m%d')
        date_list.append(date_format)

    return date_list


def make_linux_path(*path: str) -> str:
    if path[0].startswith('/'):
        return '/'.join(path)
    else:
        return '/' + '/'.join(path)


def list2str(items: list) -> str:
    return ','.join(items)


def remove_duplicates(items: list) -> list:
    return list(set(items))
