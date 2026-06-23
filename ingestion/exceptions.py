"""Исключения ingestion-процесса."""


class StationDirectoryDoesntExist(Exception):
    """Папка STATION_XX не найдена."""


class VideoAlreadyExistsError(Exception):
    """API вернул 406 — видео с таким content_hash уже есть."""


class RecognitionApiError(Exception):
    """Ошибка при обращении к API."""


class VideoFileNotValid(Exception):
    """Имя файла не подходит под ожидаемый шаблон STATION_XX/YYYYMMDD_HHMMSS.mp4."""
