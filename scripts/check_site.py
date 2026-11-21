#!/usr/bin/env python3
"""Статическая проверка ссылок, ресурсов и меню с учётом префикса сайта Pages."""
import argparse
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.parse import unquote, urljoin, urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[1]
BASE = "https://denis11134.github.io/loanflow/"


class Links(HTMLParser):
    def __init__(self, value):
        super().__init__()
        self.links, self.ids, self.canonical = [], set(), None
        self.feed(value)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        for name in ("src", "href"):
            if name in attrs:
                self.links.append(attrs[name])
        if tag == "link" and attrs.get("rel") == "canonical":
            self.canonical = attrs.get("href")


def leaves(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from leaves(child)
    else:
        for child in value:
            yield from leaves(child)


def check(site, config=ROOT/"mkdocs.yml"):
    # MkDocs содержит Python-тег функции SuperFences; нужны только обычные значения nav/site_url.
    class Loader(yaml.SafeLoader):
        pass
    Loader.add_multi_constructor("tag:yaml.org,2002:python/name:", lambda loader, suffix, node: suffix)
    settings = yaml.load(config.read_text(), Loader=Loader)
    assert settings["site_url"] == BASE
    prefix, host = urlsplit(BASE).path, urlsplit(BASE).netloc
    pages = {p.resolve(): Links(p.read_text()) for p in site.rglob("*.html")}
    checked = 0

    def resource(page_url, href):
        nonlocal checked
        url = urlsplit(urljoin(page_url, href))
        if url.scheme not in ("http", "https") or url.netloc != host:
            return
        assert url.path.startswith(prefix), f"Ссылка выходит за /loanflow/: {page_url} → {href}"
        relative = unquote(url.path[len(prefix):])
        target = (site/relative).resolve()
        assert target.is_relative_to(site.resolve()), href
        if target.is_dir():
            target /= "index.html"
        assert target.is_file(), f"Нет ресурса: {page_url} → {href}"
        if url.fragment and target in pages:
            assert unquote(url.fragment) in pages[target].ids, f"Нет якоря: {page_url} → {href}"
        checked += 1

    for path, page in pages.items():
        url = urljoin(BASE, str(path.relative_to(site.resolve())))
        for href in page.links:
            resource(url, href)
        if page.canonical:
            assert page.canonical.startswith(BASE), page.canonical
    for path in site.rglob("*.css"):
        url = urljoin(BASE, str(path.relative_to(site)))
        for href in re.findall(r"url\([\"']?([^\)\"']+)", path.read_text()):
            resource(url, href)
    nav = list(leaves(settings["nav"]))
    for source in nav:
        page = source.removesuffix(".md")
        target = site/(page+".html" if page.endswith("index") else page+"/index.html")
        assert target.is_file(), source
    search = json.loads((site/"search/search_index.json").read_text())
    assert search["docs"] and any("самозапрет" in p.get("text", "").lower() for p in search["docs"])
    for item in search["docs"]:
        resource(BASE, item["location"])
    for path in site.rglob("*"):
        assert not path.is_symlink(), path
        assert not any(part in (".env", ".git", ".cache", "test-results", "services", "mobile") for part in path.relative_to(site).parts), path
        assert path.suffix not in (".log", ".sqlite", ".db", ".py", ".pyc"), path
    assert not (site/"mobile").exists()
    return {"nav_pages": len(nav), "html_pages": len(pages), "checked_links": checked,
            "files": sum(p.is_file() for p in site.rglob("*"))}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(args.site.resolve()), ensure_ascii=False))
