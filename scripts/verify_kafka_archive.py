"""Сверка SHA-512 дистрибутива с файлом контрольной суммы Apache."""
import hashlib
from pathlib import Path
import re
import sys

archive, checksum = map(Path, sys.argv[1:])
expected = "".join(re.findall(r"\b[0-9a-fA-F]{8,128}\b", checksum.read_text()))
with archive.open("rb") as stream:
    actual = hashlib.file_digest(stream, "sha512").hexdigest()
if len(expected) != 128 or actual != expected.lower():
    raise SystemExit("Контрольная сумма Apache Kafka не совпала")
print("SHA-512 Apache Kafka подтверждена")
