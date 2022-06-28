import datetime
import dateutil.parser as dparser
import logging
import os
import shutil
from dataclasses import dataclass

from keepa.exceptions import NotADirectoryException


@dataclass
class FileToUpload:

    # Фактический путь до файла в системе
    file_path: str
    # Ключ идентификации файла в S3
    file_key: str


class Loader:
    """Загрузчик файлов в storage."""

    def __init__(self, client, entries):
        self._client = client
        self._entries = entries
        # Временные файлы, созданные скриптом, и подлежащие
        # удалению по окончанию загрузки файлов.
        self._trash = []

    @staticmethod
    def _append_date(file_path):
        """Дописывает дату префиксом к имени файла по необходимости."""
        file_name = os.path.basename(file_path)
        try:
            # Пытаемся узнать, есть ли у файла дата в названии.
            # Если нет, добавим сами.
            dparser.parse(file_name, fuzzy=True)
        except dparser.ParserError:
            path, ext = os.path.splitext(file_path)
            postfix = datetime.date.today().strftime('%d-%m-%Y')
            return f'{path}-{postfix}{ext}'
        else:
            return file_path

    def _create_archive(self, dir_path, append_date) -> list[FileToUpload]:
        """Создает архив директории на загрузку."""
        if append_date:
            output_filename = self._append_date(dir_path)
        else:
            output_filename = dir_path
        archive_path = shutil.make_archive(output_filename, 'zip', dir_path)
        self._trash.append(archive_path)
        file_key = os.path.basename(archive_path)
        result = FileToUpload(
            file_path=archive_path,
            file_key=file_key
        )
        return [result]

    def _get_dir_files(self, dir_path, append_date) -> list[FileToUpload]:
        """Собирает файлы для загрузки из директории."""
        files_to_upload = []
        # Запоминаем имя указанной директории.
        # В дальнейшем, если директория имеет глубину больше 1,
        # ключи файлов будут относительными путями из этой директории
        root_dir_name = os.path.basename(dir_path)
        for path, directory, files in os.walk(dir_path):
            relative_path = path.split(root_dir_name)[1]
            for file in files:
                file_key = os.path.join(relative_path, file)
                # Убираем начальный слэш, чтобы не казалось,
                # что это абсолютный путь
                if file_key.startswith('/'):
                    file_key = file_key[1:]
                files_to_upload.append(
                    FileToUpload(
                        file_path=os.path.join(path, file),
                        file_key=(
                            self._append_date(file_key)
                            if append_date else file_key
                        )
                    )
                )
        return files_to_upload

    def _get_files_to_upload(self, entry) -> list[FileToUpload]:
        """Собирает файлы для загрузки в зависимости от режима."""
        archive_mode = entry.get('archive_mode', False)
        append_date = entry.get('append_date', False)
        path = entry['path']
        if archive_mode:
            files = self._create_archive(path, append_date)
        else:
            files = self._get_dir_files(path, append_date)
        return files

    def _make_upload(
        self, files_to_upload: list[FileToUpload], bucket
    ) -> tuple[list, list]:
        """Загружает файлы в storage по одному."""
        uploaded_files = []
        failed_files = []
        for file in files_to_upload:
            try:
                self._client.upload_file(file.file_path, bucket, file.file_key)
            except Exception as exc:
                logging.exception(
                    f'Произошла ошибка при загрузке файла {file.file_path}'
                )
                failed_files.append(file)
            else:
                uploaded_files.append(file)
        return uploaded_files, failed_files

    def _validate_entry(self, entry):
        # Проверка, является ли указанный путь директорией
        path = entry['path']
        if not os.path.isdir(path):
            raise NotADirectoryException(path)
        # Проверка глубины директории, если отключен режим архивации.
        # Это нужно чтобы случайно не загрузить кучу файлов по одному.
        all_files = all(
            os.path.isfile(os.path.join(path, file))
            for file in os.listdir(path)
        )
        archive_mode = entry.get('archive_mode', False)
        if not all_files and not archive_mode:
            logging.warning(
                """
                Глубина директории больше 1, но режим архивации не включен. 
                Есть опасность нерациональной траты запросов.
                """
            )

    @staticmethod
    def _clear_files(files: list[FileToUpload]):
        """Удаляет загруженные файлы из директории."""
        for file in files:
            os.remove(file.file_path)

    def _remove_trash(self):
        for file in self._trash:
            os.remove(file)

    def upload(self):
        uploaded_files = []
        failed_files = []
        for entry in self._entries:
            try:
                self._validate_entry(entry)
            except NotADirectoryException as exc:
                logging.error(str(exc))
                continue
            files = self._get_files_to_upload(entry)
            bucket = entry['bucket_name']
            uploaded, failed = self._make_upload(files, bucket)
            uploaded_files.extend(uploaded)
            failed_files.extend(failed)
            if entry.get('delete_after', False):
                path = entry['path']
                self._clear_files(uploaded_files)
        result_msg = (f"""
            Успешно загружено: {len(uploaded_files)} \n,
            Не удалось загрузить: {len(failed_files)}. \n
        """)
        logging.info(result_msg)
        self._remove_trash()
