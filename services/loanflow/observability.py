"""Команда просмотра диагностики без потоков и тяжёлых импортов при запуске."""
import argparse
import asyncio
import json
import os
import time

from .probes import run_probe


def main():
    parser = argparse.ArgumentParser(description="Безопасный снимок внутреннего состояния стенда")
    parser.add_argument("--url", action="append", required=True, help="URL API или management обработчика")
    parser.add_argument("--watch", action="store_true", help="Повторять каждые 5 секунд")
    args = parser.parse_args()

    async def collect():
        async def read(url):
            try:
                value = await run_probe("http", {"url": url.rstrip("/")+"/internal/diagnostics",
                                                "token": os.environ.get("LOANFLOW_MONITOR_TOKEN")}, timeout=1.5)
                return {"ready": True, "values": value}
            except Exception:
                return {"ready": False, "category": "unavailable"}
        return await asyncio.gather(*(read(url) for url in args.url))

    while True:
        started = time.monotonic()
        print(json.dumps(asyncio.run(collect()), ensure_ascii=False), flush=True)
        if not args.watch:
            break
        time.sleep(max(0, 5-(time.monotonic()-started)))


if __name__ == "__main__":
    main()
