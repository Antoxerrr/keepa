import logging
import os
import sys
from pathlib import Path

import boto3

from config import get_config
from extensions import DateStorageHandler
from loader import Loader


SOURCES_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SOURCES_DIR.parent

CONFIG = get_config(PROJECT_DIR)
session = boto3.session.Session()
s3 = session.client(**CONFIG['client_settings'])

logging.basicConfig(
    format='%(levelname)s: %(asctime)s %(message)s', level=logging.INFO,
    filename=os.path.join(PROJECT_DIR, 'logs.log'), encoding='utf-8'
)

if __name__ == '__main__':
    logging.info('Запущена отправка в хранилище')

    entries = CONFIG['entries']

    # Грузим файлы в storage
    loader = Loader(s3, entries)
    loader.upload()

    # По необходимости обрабатываем хранение по датам
    for entry in entries:
        if entry.get('date_handling', False):
            dsh = DateStorageHandler(s3, entry.get('bucket_name'))
            dsh.handle()
