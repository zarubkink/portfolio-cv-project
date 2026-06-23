import hashlib
from pathlib import Path
from typing import BinaryIO

FileOrPath = str | Path | BinaryIO


def hash_large_file(
    file: FileOrPath,
    algorithm=hashlib.sha256,
    chunk_size: int = 65536,
) -> bytes:
    """SHA-256 (или другой) хэш файла.

    Возвращает сырые байты (32 байта для SHA-256), чтобы хранить в BYTEA,
    а не в 64-символьной hex-строке (см. PROJECT_SCAFFOLD_PROMPT § 3.3.B).

    Принимает путь (str | Path) или уже открытый binary stream.
    """
    hasher = algorithm()

    if isinstance(file, (str, Path)):
        path = Path(file)
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hasher.update(chunk)
    else:
        for chunk in iter(lambda: file.read(chunk_size), b""):
            hasher.update(chunk)
        try:
            file.seek(0)
        except (AttributeError, OSError):
            pass

    return hasher.digest()


def hash_large_file_hex(file: FileOrPath, **kwargs) -> str:
    """Hex-представление хэша — для логов и человекочитаемых мест."""
    return hash_large_file(file, **kwargs).hex()
