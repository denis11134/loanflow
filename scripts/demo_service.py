"""Короткий HTTP-прогон синтетического сценария подачи и публикации."""
import argparse
import hashlib
import json
from pathlib import Path
from time import monotonic, sleep
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tokens", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    token = json.loads(args.tokens.read_text())["mobile"]

    def call(method, path, expected, data=None, key=None, pdf=False):
        headers = {"Authorization": f"Bearer {token}"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if key:
            headers["Idempotency-Key"] = str(key)
        request = Request(args.url + path, headers=headers, method=method,
                          data=json.dumps(data).encode() if data is not None else None)
        with urlopen(request, timeout=5) as response:
            if response.status != expected:
                raise RuntimeError(f"HTTP {response.status}, ожидался {expected}")
            return response.read() if pdf else json.load(response)

    try:
        draft = call("POST", "/applications", 201, {"requested_amount_minor": "5000000",
                     "requested_term_months": 12}, uuid4())
        path = "/applications/" + draft["application_id"]
        receipt = call("POST", path + "/submission", 202)
        if call("POST", path + "/submission", 202) != receipt:
            raise RuntimeError("Изменилось подтверждение подачи")
        end = monotonic() + 20
        while monotonic() < end:
            current = call("GET", path, 200)
            if current["offer_availability"] == "AVAILABLE":
                break
            if current["status"] not in ("SCORING", "APPROVED"):
                raise RuntimeError(f"Итог оценки {current['status']}")
            sleep(0.25)
        else:
            raise RuntimeError("ИУ не опубликованы за 20 секунд")
        offer = call("GET", path + "/offer", 200)
        document = call("GET", f"{path}/terms/{offer['terms_id']}/document", 200, pdf=True)
        if hashlib.sha256(document).hexdigest() != offer["document_sha256"]:
            raise RuntimeError("Хеш PDF не совпал")
        print(f"{draft['application_id']} DRAFT → SCORING → APPROVED")
        print(f"Повтор подачи совпал, SHA-256 PDF подтверждён, ИУ действуют до {offer['offer_expires_at']}")
    except HTTPError as exc:
        raise SystemExit(f"Сервис вернул HTTP {exc.code}") from None


if __name__ == "__main__":
    main()
