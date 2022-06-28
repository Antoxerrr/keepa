import datetime
from dataclasses import dataclass
from itertools import groupby

import dateutil.parser as dparser


@dataclass
class FileKey:

    file_key: str
    file_date: datetime.datetime


class DateStorageHandler:
    """
    Класс, отвечающий за оптимальное хранение файлов по датам.

    Цель - за каждый прошедший месяц оставлять только по
    одному, самому актуальному файлу.
    """

    def __init__(self, client, bucket):
        self._client = client
        self._bucket = bucket
        self._to_delete = []

    def _get_file_keys(self):
        """Достает ключи файлов из бакета."""
        response = self._client.list_objects(Bucket=self._bucket)
        data = response.get('Contents', [])
        return [file['Key'] for file in data]

    @staticmethod
    def _group_key(file_key: FileKey):
        return file_key.file_date.month, file_key.file_date.year

    @staticmethod
    def _sort_key(file_key: FileKey):
        return file_key.file_date

    @staticmethod
    def _process_file_keys(file_keys):
        """
        Обрабатывает ключи файлов.

        Принимает сырой массив с ключами, возвращает удобную структуру
        с ключом файла и вытащенной из него датой, при этом выбрасывает
        файлы без дат и файлы за текущий месяц.
        """
        handled = []
        today = datetime.date.today()
        for key in file_keys:
            try:
                date = dparser.parse(key, fuzzy=True)
            except dparser.ParserError:
                # Если произошла ошибка, значит в файле не указана дата.
                # В таком случае просто такой файл пока не трогаем.
                continue
            else:
                # Пропускаем файлы за текущий месяц
                if date.month != today.month and date.year != today.year:
                    handled.append(FileKey(file_key=key, file_date=date))
        return handled

    def _find_files_to_delete(self, file_keys: list[FileKey]):
        for key, group in groupby(file_keys, key=self._group_key):
            # Сортируем по убыванию
            sorted_group = sorted(group, key=self._sort_key, reverse=True)
            # Первым индексом будет сама большая дата.
            # Все остальные ставим на удаление.
            self._to_delete.extend(sorted_group[1:])

    def _delete_files(self):
        keys_to_delete = [
            dict(Key=file_key.file_key) for file_key in self._to_delete
        ]
        self._client.delete_objects(
            Bucket=self._bucket, Delete={'Objects': keys_to_delete}
        )

    def handle(self):
        file_keys = self._process_file_keys(self._get_file_keys())
        self._find_files_to_delete(file_keys)
        self._delete_files()
