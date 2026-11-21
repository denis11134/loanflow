#!/usr/bin/env python3
"""Одинаковая проверка локального /loanflow/ и публичного GitHub Pages."""
import argparse
from contextlib import contextmanager, ExitStack
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import time
from pathlib import Path
from threading import Thread
from urllib.parse import urljoin, urlsplit
from urllib.request import urlopen

from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[1]
DIAGRAMS = {
    "04-data/2.1-logical-er-model/": 2,
    "06-integrations/4.1-integration-architecture/": 1,
    "06-integrations/4.1-application-assessment/": 1,
    "06-integrations/4.1-abs-disbursement/": 1,
    "07-architecture/5.1-service-decomposition/": 2,
}


@contextmanager
def serve(site):
    class Handler(SimpleHTTPRequestHandler):
        def do_GET(self):
            if not self.path.startswith("/loanflow/"):
                self.send_error(404)
                return
            self.path = self.path[len("/loanflow"):]
            super().do_GET()

        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(Handler, directory=str(site)))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/loanflow/"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)


def check(base, screenshots, expected_sha=None):
    base = base.rstrip("/")+"/"
    host = urlsplit(base).netloc
    screenshots.mkdir(parents=True, exist_ok=True)
    if expected_sha:
        deadline = time.monotonic()+180
        while True:
            try:
                with urlopen(urljoin(base, "build-info.json?commit="+expected_sha), timeout=10) as response:
                    if json.load(response)["commit"] == expected_sha:
                        break
            except (OSError, ValueError, KeyError):
                pass
            assert time.monotonic() < deadline, "Публичный сайт содержит другой SHA или недоступен"
            time.sleep(3)
    checked, errors, failed_resources = [], [], []
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(ROOT/".cache/ms-playwright"))
    with sync_playwright() as pw, ExitStack() as cleanup:
        browser = pw.chromium.launch()
        cleanup.callback(browser.close)
        context = browser.new_context(viewport={"width": 1280, "height": 900}, locale="ru-RU")
        cleanup.callback(context.close)
        # Material 9.8 помещает SVG в closed Shadow DOM. Сохраняем ссылку только
        # для проверок, не меняя mode и поведение самого attachShadow.
        context.add_init_script("""(() => {
            const roots = new WeakMap();
            const attach = Element.prototype.attachShadow;
            Element.prototype.attachShadow = function(options) {
                const root = attach.call(this, options); roots.set(this, root); return root;
            };
            window.__diagramRoots = roots;
        })();""")
        page = context.new_page()
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("response", lambda response: failed_resources.append(f"{response.status} {response.url}")
                if response.status >= 400 and urlsplit(response.url).netloc == host else None)
        page.on("requestfailed", lambda request: failed_resources.append(request.url)
                if urlsplit(request.url).netloc == host else None)

        def visit(path):
            response = page.goto(urljoin(base, path), wait_until="networkidle")
            assert response and response.status == 200, path
            expect(page.locator("h1")).to_be_visible()
            checked.append(path)

        def diagrams(count):
            page.wait_for_function("""count => {
                const hosts = [...document.querySelectorAll('.mermaid')];
                return hosts.length === count && hosts.every(host => {
                    const svg = window.__diagramRoots.get(host)?.querySelector('svg');
                    return svg && !svg.querySelector('.error-icon') && svg.getBoundingClientRect().height > 0
                        && host.tabIndex === 0;
                });
            }""", arg=count, timeout=45000)
            metrics = page.evaluate("""() => [...document.querySelectorAll('.mermaid')].map(host => {
                const svg = window.__diagramRoots.get(host).querySelector('svg');
                const box = svg.getBoundingClientRect();
                const scale = svg.viewBox.baseVal.width ? box.width/svg.viewBox.baseVal.width : 1;
                host.scrollLeft = host.scrollWidth-host.clientWidth;
                const end = svg.getBoundingClientRect().right;
                const right = host.getBoundingClientRect().right;
                const result = {width: box.width, viewport: host.clientWidth, scrollWidth: host.scrollWidth,
                    overflow: getComputedStyle(host).overflowX, scale, endVisible: end <= right+2,
                    scrollLeft: host.scrollLeft};
                host.scrollLeft = 0;
                return result;
            })""")
            for index, metric in enumerate(metrics):
                assert metric["scale"] >= .98, ("Подписи уменьшены", metric)
                assert metric["endVisible"], ("Правый край недоступен", metric)
                if metric["width"] > metric["viewport"]+2:
                    assert metric["overflow"] in ("auto", "scroll") and metric["scrollLeft"] > 0, metric
                    region = page.locator(".mermaid").nth(index)
                    region.focus()
                    page.keyboard.press("ArrowRight")
                    page.wait_for_function("i => document.querySelectorAll('.mermaid')[i].scrollLeft > 0", arg=index)
                    region.evaluate("node => { node.scrollLeft = 0 }")
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            return metrics

        visit("")
        expect(page.locator(".md-source").first).to_have_attribute("href", "https://github.com/denis11134/loanflow")
        links = page.locator(".md-nav--primary a.md-nav__link[href]").evaluate_all("nodes => nodes.map(n => n.href)")
        nav_urls = sorted({url.split("#")[0] for url in links if urlsplit(url).netloc == host})
        for url in nav_urls:
            assert urlsplit(url).path.startswith("/loanflow/"), url
            with urlopen(url, timeout=10) as response:
                assert response.status == 200
        page.screenshot(path=str(screenshots/"home.png"), full_page=True)

        search = page.locator("input.md-search__input")
        search.click()
        expect(page.locator(".md-search-result__meta")).not_to_contain_text("Инициализация", timeout=20000)
        search.fill("самозапрет")
        # Unicode insertText Playwright не создаёт keydown. End оставляет запрос
        # неизменным и запускает штатную клавиатурную обработку поиска Material.
        search.press("End")
        try:
            expect(page.locator(".md-search-result__item").first).to_be_visible(timeout=20000)
        except AssertionError:
            page.screenshot(path=str(screenshots/"search-failure.png"), full_page=True)
            print(json.dumps({"search_meta": page.locator(".md-search-result__meta").inner_text(),
                              "query": search.input_value(), "errors": errors,
                              "failed_resources": failed_resources}, ensure_ascii=False))
            raise
        expect(page.locator(".md-search-result__list")).to_contain_text("самозапрет", ignore_case=True)
        result = page.locator(".md-search-result__link").first
        result.click()
        expect(page.locator("h1")).to_be_visible()
        assert page.url.startswith(base)

        for path, count in DIAGRAMS.items():
            visit(path)
            diagrams(count)
        page.screenshot(path=str(screenshots/"architecture.png"), full_page=True)
        page.locator(".mermaid").last.screenshot(path=str(screenshots/"c2-left.png"))
        page.locator(".mermaid").last.evaluate("node => { node.scrollLeft = node.scrollWidth }")
        page.screenshot(path=str(screenshots/"architecture-right.png"), full_page=True)
        page.locator(".mermaid").last.screenshot(path=str(screenshots/"c2-right.png"))

        visit("07-architecture/5.4-assessment-orchestration/")
        expect(page.get_by_role("link", name="diagrams/loanflow-assessment.bpmn", exact=True)).to_have_attribute(
            "href", "https://github.com/denis11134/loanflow/blob/main/diagrams/loanflow-assessment.bpmn")
        response = page.goto(urljoin(base, "08-prototypes/loanflow-demo.html"), wait_until="domcontentloaded")
        assert response and response.status == 200
        expect(page.locator("#amount")).to_be_visible()
        page.screenshot(path=str(screenshots/"prototype.png"), full_page=True)
        checked.append("08-prototypes/loanflow-demo.html")

        page.set_viewport_size({"width": 375, "height": 850})
        visit("07-architecture/5.1-service-decomposition/")
        diagrams(2)
        page.screenshot(path=str(screenshots/"architecture-mobile.png"), full_page=True)
        page.locator(".mermaid").last.evaluate("node => { node.scrollLeft = node.scrollWidth }")
        page.locator(".mermaid").last.screenshot(path=str(screenshots/"c2-mobile-right.png"))
        visit("")
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        page.locator(".md-header label[for='__drawer']").click()
        expect(page.locator(".md-sidebar--primary")).to_be_visible()
        section = page.locator(".md-nav--primary").get_by_role("link", name="Требования", exact=True).locator("..").locator("label")
        toggle = page.locator("#"+section.get_attribute("for"))
        if not toggle.is_checked():
            section.click()
        page.locator(".md-nav--primary").get_by_role("link", name="1.6. Требования MVP", exact=True).click()
        expect(page.locator("h1")).to_contain_text("Требования")
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        page.screenshot(path=str(screenshots/"mobile.png"), full_page=True)
        response = page.goto(urljoin(base, "08-prototypes/loanflow-demo.html"), wait_until="domcontentloaded")
        assert response.status == 200
        prototype = page.evaluate((ROOT/"tests/prototype_journey.js").read_text())
        assert prototype["passed"] > 0
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        assert not failed_resources, failed_resources
        assert not errors, errors
    return {"base_url": base, "nav_pages": len(nav_urls), "browser_pages": checked,
            "mermaid_diagrams": sum(DIAGRAMS.values()), "search": "самозапрет", "mobile_width": 375,
            "prototype_checks": prototype["passed"], "diagram_horizontal_access": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--site", type=Path)
    source.add_argument("--url")
    parser.add_argument("--expected-sha")
    parser.add_argument("--screenshots", type=Path, default=ROOT/"test-results/pages")
    args = parser.parse_args()
    if args.site:
        with serve(args.site.resolve()) as url:
            result = check(url, args.screenshots, args.expected_sha)
    else:
        result = check(args.url, args.screenshots, args.expected_sha)
    print(json.dumps(result, ensure_ascii=False))
