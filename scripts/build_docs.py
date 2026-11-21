#!/usr/bin/env python3
"""Сборка только отслеживаемой документации указанного коммита для GitHub Pages."""
import argparse
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def build(output, revision="HEAD"):
    sha = subprocess.check_output(["git", "rev-parse", revision+"^{commit}"], cwd=ROOT, text=True).strip()
    paths = subprocess.check_output(["git", "ls-tree", "--name-only", sha, "--", "docs", "mkdocs.yml", "overrides"],
                                    cwd=ROOT, text=True).splitlines()
    archive = subprocess.check_output(["git", "archive", sha, "--", *paths], cwd=ROOT)
    with tempfile.TemporaryDirectory(prefix="loanflow-docs-") as directory:
        source = Path(directory)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            for member in tar.getmembers():
                path = Path(member.name)
                if (path.is_absolute() or ".." in path.parts or path.parts[0] not in ("docs", "mkdocs.yml", "overrides")
                        or not (member.isdir() or member.isfile())):
                    raise ValueError("Недопустимый источник статической публикации")
            tar.extractall(source, filter="data")
        subprocess.run([sys.executable, "-m", "mkdocs", "build", "--strict", "--config-file",
                        str(source/"mkdocs.yml"), "--site-dir", str(output.resolve())], check=True, cwd=source)
    (output/"build-info.json").write_text(json.dumps({"commit": sha,
        "repository": "https://github.com/denis11134/loanflow"}, indent=2)+"\n")
    (output/".nojekyll").touch()
    return sha


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT/"site")
    parser.add_argument("--revision", default="HEAD")
    args = parser.parse_args()
    print(build(args.output, args.revision))
