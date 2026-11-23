"""Строгая сборка, полнота меню и реальные локальные ссылки готового сайта."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit
import re
import subprocess
import sys
import markdown
from markdown.extensions.toc import slugify_unicode
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
            prefix = urlsplit(config['site_url']).path
            if url.path.startswith('/'):
                assert url.path.startswith(prefix), (str(page.relative_to(site)), href)
            target = ((site / unquote(url.path)[len(prefix):]) if url.path.startswith('/')
                      else page.parent / unquote(url.path)).resolve() if url.path else page
            if target.is_dir():
                target /= 'index.html'
            assert target.exists(), (str(page.relative_to(site)), href)
            if url.fragment and target in parsed:
                assert unquote(url.fragment) in parsed[target].ids, (str(page.relative_to(site)), href)
    readme = (ROOT / 'README.md').read_text()
    # README открывается на GitHub, документация — в MkDocs. Проверяем также
    # якоря переходов между ними: перенос инструкций не должен ломать старые ссылки.
    readme_html = markdown.markdown(readme, extensions=['toc', 'tables'],
        extension_configs={'toc': {'slugify': slugify_unicode}})
    readme_ids = Links(readme_html).ids
    for href in re.findall(r'\]\(([^)]+)\)', readme):
        url = urlsplit(href)
        if url.scheme or url.netloc:
            continue
        target = (ROOT / unquote(url.path)).resolve() if url.path else ROOT / 'README.md'
        assert target.exists(), href
        if url.fragment:
            if target == ROOT / 'README.md':
                ids = readme_ids
            elif target.suffix == '.md' and target.is_relative_to(ROOT / 'docs'):
                relative = target.relative_to(ROOT / 'docs')
                html = (site / relative.parent / 'index.html' if target.stem == 'index'
                        else site / relative.with_suffix('') / 'index.html')
                ids = parsed[html].ids
            else:
                continue
            assert unquote(url.fragment) in ids, href
    for data in parsed.values():
        for href in data.links:
            url = urlsplit(href)
            if (url.netloc == 'github.com' and
                url.path.rstrip('/') in ('/denis11134/loanflow', '/denis11134/loanflow/blob/main/README.md') and
                url.fragment and url.fragment != 'readme'):
                assert unquote(url.fragment) in readme_ids, href
