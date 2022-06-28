class UploadException(Exception):
    """Базовое исключение загрузки в S3."""

    msg: str

    def __str__(self):
        return self.msg


class NotADirectoryException(Exception):
    """Исключение 'Не является директорией'."""

    def __init__(self, path):
        self.msg = f'{path} не является директорией'
