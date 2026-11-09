"""Строгая сборка, полнота меню и реальные локальные ссылки готового сайта."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit
import re
import subprocess
import sys
from mkdocs.config import load_config
from conftest import ROOT


class Links(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.links = []
        self.ids = set()
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            self.ids.add(attrs['id'])
        for attr in ('href', 'src'):
            if attr in attrs:
                self.links.append(attrs[attr])


def test_docs_build_navigation_and_links(tmp_path):
    site = tmp_path / 'site'
    subprocess.run([sys.executable, '-m', 'mkdocs', 'build', '--strict', '-f', str(ROOT / 'mkdocs.yml'),
                    '--site-dir', str(site)], cwd=ROOT, check=True)
    config = load_config(str(ROOT / 'mkdocs.yml'))
    def leaves(value):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for child in value.values():
                yield from leaves(child)
        else:
            for child in value:
                yield from leaves(child)
    pages = {str(p.relative_to(ROOT / 'docs')) for p in (ROOT / 'docs').rglob('*.md')}
    assert pages == set(leaves(config['nav']))
    parsed = {p: Links(p.read_text()) for p in site.rglob('*.html')}
    for page, data in parsed.items():
        for href in data.links:
            url = urlsplit(href)
            if url.scheme or url.netloc or not url.path and not url.fragment:
                continue
            target = ((site / unquote(url.path).lstrip('/')) if url.path.startswith('/')
                      else page.parent / unquote(url.path)).resolve() if url.path else page
            if target.is_dir():
                target /= 'index.html'
            assert target.exists(), (str(page.relative_to(site)), href)
            if url.fragment and target in parsed:
                assert unquote(url.fragment) in parsed[target].ids, (str(page.relative_to(site)), href)
    for target in re.findall(r'\]\(([^)]+)\)', (ROOT / 'README.md').read_text()):
        if not urlsplit(target).scheme:
            assert (ROOT / target.split('#')[0]).exists(), target
