"""Загрузка, проверка и распаковка Camunda 8 Run без Docker."""
import argparse
from pathlib import Path
import shutil
import tarfile
import tempfile
from urllib.request import urlopen

from verify_camunda_archive import ARCHIVE_NAME, ARCHIVE_URL, VERSION, verify_archive


def prepare(cache_dir):
    cache_dir = Path(cache_dir).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    archive = cache_dir / ARCHIVE_NAME
    if not archive.exists():
        partial = archive.with_suffix(archive.suffix + ".partial")
        with urlopen(ARCHIVE_URL, timeout=120) as response, partial.open("wb") as stream:
            shutil.copyfileobj(response, stream, length=1024 * 1024)
        partial.replace(archive)
    verify_archive(archive)
    home = cache_dir / f"c8run-{VERSION}"
    if not (home / "c8run").exists():
        with tempfile.TemporaryDirectory(prefix="camunda-extract-", dir=cache_dir) as temporary:
            with tarfile.open(archive, "r:gz") as stream:
                stream.extractall(temporary, filter="data")
            extracted = Path(temporary) / home.name
            if not (extracted / "c8run").is_file():
                raise ValueError("Неожиданная структура архива Camunda 8 Run")
            extracted.rename(home)
    return home


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/camunda-8.10.9"))
    args = parser.parse_args()
    print(prepare(args.cache_dir))
