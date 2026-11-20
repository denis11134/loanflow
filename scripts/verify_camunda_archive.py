"""Проверка закреплённого Linux x86_64 дистрибутива Camunda 8 Run."""
import hashlib
from pathlib import Path
import sys

VERSION = "8.10.9"
ARCHIVE_NAME = f"camunda8-run-{VERSION}-linux-x86_64.tar.gz"
ARCHIVE_URL = f"https://github.com/camunda/camunda/releases/download/{VERSION}/{ARCHIVE_NAME}"
ARCHIVE_SIZE = 931744859
ARCHIVE_SHA256 = "d6e269d6ce928be19f810294c45db00b7b43f42207a5a96a970635efacfc738f"


def verify_archive(archive):
    archive = Path(archive)
    if archive.stat().st_size != ARCHIVE_SIZE:
        raise ValueError("Размер архива Camunda 8 Run 8.10.9 не совпал")
    with archive.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != ARCHIVE_SHA256:
        raise ValueError("SHA-256 архива Camunda 8 Run 8.10.9 не совпала")


if __name__ == "__main__":
    verify_archive(sys.argv[1])
    print("SHA-256 Camunda 8 Run 8.10.9 подтверждена")
