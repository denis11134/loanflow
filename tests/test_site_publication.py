"""Сайт публикует только отслеживаемые материалы и сохраняет проектный путь."""
import importlib.util
import json
import hashlib
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT/"scripts"/(name+".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


@pytest.fixture
def repository(tmp_path):
    root = tmp_path/"repository"
    root.mkdir()
    def git(*args):
        return subprocess.check_output(["git", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", *args],
                                       cwd=root, text=True).strip()
    git("init", "-q")
    git("config", "user.name", "Publication test")
    git("config", "user.email", "test@example.invalid")
    (root/"docs").mkdir()
    (root/"mkdocs.yml").write_text("site_name: Test\ndocs_dir: docs\n")
    (root/"docs/index.md").write_text("# Committed documentation\n")
    git("add", "docs", "mkdocs.yml")
    git("commit", "-qm", "docs: fixture")
    return root, git


def test_publication_ignores_dirty_and_untracked_materials(repository, tmp_path):
    root, git = repository
    builder = module("build_docs")
    builder.ROOT = root
    (root/"docs/index.md").write_text("# Uncommitted secret marker\n")
    (root/"docs/local-token.txt").write_text("untracked-token-marker")
    (root/".env").write_text("local-secret-marker")
    (root/"services/mobile").mkdir(parents=True)
    (root/"services/mobile/index.html").write_text("uncommitted-mobile-marker")
    output = tmp_path/"site"
    sha = builder.build(output)
    assert sha == git("rev-parse", "HEAD")
    assert json.loads((output/"build-info.json").read_text())["commit"] == sha
    assert "Committed documentation" in (output/"index.html").read_text()
    assert not (output/"local-token.txt").exists()
    markers = ("Uncommitted secret marker", "untracked-token-marker", "local-secret-marker", "uncommitted-mobile-marker")
    assert not any(marker in path.read_text(errors="replace") for path in output.rglob("*") if path.is_file() for marker in markers)


def test_publication_rejects_tracked_symlinks(repository, tmp_path):
    root, git = repository
    (root/"docs/outside").symlink_to(tmp_path/".env")
    git("add", "docs/outside")
    git("commit", "-qm", "test: symlink")
    builder = module("build_docs")
    builder.ROOT = root
    with pytest.raises(ValueError, match="источник"):
        builder.build(tmp_path/"site")


def test_project_root_escape_is_rejected(tmp_path):
    checker = module("check_site")
    site = tmp_path/"site"
    site.mkdir()
    (site/"index.html").write_text('<a href="/api/loanflow.openapi.yaml">API</a>')
    config = tmp_path/"mkdocs.yml"
    config.write_text("site_url: https://denis11134.github.io/loanflow/\nnav: [index.md]\n")
    with pytest.raises(AssertionError, match="/loanflow/"):
        checker.check(site, config)


def test_diagram_runtime_matches_committed_checksum():
    vendor = ROOT/"docs/assets/vendor"
    info = json.loads((vendor/"mermaid.json").read_text())
    assert info["version"] == "11.17.2"
    assert info["license"] == "MIT" and (vendor/"mermaid.LICENSE.txt").is_file()
    assert hashlib.sha256((vendor/("mermaid-"+info["version"]+".min.js")).read_bytes()).hexdigest() == info["sha256"]
