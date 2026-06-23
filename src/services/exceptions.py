"""Доменные исключения agro-tracking."""


class VideoProcessError(Exception):
    """Ошибка при обработке видео (CV-стадия, чтение файла и т.п.)."""


class DuplicateVideoError(Exception):
    """Видео с таким content_hash уже есть в БД."""


class ArucoDecodeError(VideoProcessError):
    """Ошибка декодирования ArUco-маркера (не критическая — пропускаем кадр)."""


def is_retriable_without_limit(exc: Exception) -> bool:
    """Видео, которые надо ретраить бесконечно (без increment_retry_count).

    Копия паттерна из sbr/src/services/exceptions.py.
    """
    return isinstance(exc, VideoProcessError)
