"""Изолированный стенд для приёмки Postman. Запуск описан в README.md."""
import argparse
from contextlib import contextmanager
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
from secrets import randbelow, token_hex, token_urlsafe
import shutil
import subprocess
from tempfile import TemporaryDirectory
from threading import Event, Thread
from unittest.mock import patch
from urllib.request import Request, urlopen
from uuid import UUID

from psycopg.conninfo import make_conninfo

from services.loanflow import assessment, calculations, signing
from services.loanflow.abs import create_abs
from services.loanflow.api import create_app
from services.loanflow.check_stubs import create_stubs
from services.loanflow.checks import ChecksClient
from services.loanflow.seed import DEMO, seed
from services.loanflow.store import connect, migrate, one
from services.loanflow.worker import AbsClient, Worker
from test_database import temporary_postgres
from test_service import http_server


HERE = Path(__file__).resolve().parent
COLLECTION = HERE / "loanflow.postman_collection.json"
TEMPLATE = HERE / "local.postman_environment.json"
BOUNDARY_PAYMENTS = {"psk_below": 122499, "psk_at": 122500, "psk_above": 122501}


def build_scenarios(path):
    data = json.loads(DEMO.read_text())
    default = data["customers"]["mobile"]
    scenarios = {
        "manual": {},
        "refusal": {"payroll_income_rub": None},
        "error": {"payroll_income_rub": None},
        "pdn_at": {"payroll_income_rub": "100000.00", "existing_payments_rub": "45368.27"},
        "pdn_over": {"payroll_income_rub": "100000.00", "existing_payments_rub": "45378.27"},
        **{name: {} for name in BOUNDARY_PAYMENTS},
        "pdf": {},
    }
    for number, (name, overrides) in enumerate(scenarios.items(), start=2):
        customer = deepcopy(default)
        customer.update(customer_id=str(UUID(int=UUID(default["customer_id"]).int + number - 1)),
                        account_ref=f"stand-account-{number}", profile_ref=f"stand-profile-{number}")
        customer["scenario"].update(overrides)
        if name == "error":
            del customer["scenario"]["existing_payments_rub"]
        if name in BOUNDARY_PAYMENTS:
            customer["scenario"]["acceptance_payment_minor"] = BOUNDARY_PAYMENTS[name]
        data["customers"][name] = customer
        data["actors"][name] = {"role": "CUSTOMER", "customer": name}
    path.write_text(json.dumps(data), encoding="utf-8")


@contextmanager
def fixture_worker(dsn, abs_port, checks_port, sms_code):
    calculate_schedule = assessment.calculate_schedule

    def calculate_with_fixture(db, app_id, inputs):
        payment = inputs["scenario"].get("acceptance_payment_minor")
        if payment is None:
            return calculate_schedule(db, app_id, inputs)
        assert inputs["requested_amount_rub"] == 1200 and inputs["requested_term_months"] == 1
        with patch.object(calculations, "annuity_schedule", return_value=(payment, [payment], payment - 120000)):
            return calculate_schedule(db, app_id, inputs)

    def enable_posting(name, instruction):
        if name == "before_started_commit":
            with connect(dsn) as db:
                db.execute("INSERT INTO training_abs.control VALUES (%s,'SEND','AUTO_POST') ON CONFLICT DO NOTHING",
                           (instruction["instruction_id"],))

    stop = Event()
    worker = Worker(dsn, abs_client=AbsClient(f"http://127.0.0.1:{abs_port}"), checkpoint=enable_posting,
                    checks_client=ChecksClient(f"http://127.0.0.1:{checks_port}"))

    def run():
        while not stop.is_set():
            if not worker.run_once():
                stop.wait(0.1)

    with patch.object(assessment, "calculate_schedule", calculate_with_fixture), \
            patch.object(signing, "stand_code", return_value=sms_code):
        thread = Thread(target=run, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("Воркер приёмки не завершился")


def prepare_environment(path, tokens, base_url, sms_code):
    env = json.loads(TEMPLATE.read_text())
    env["name"] = "LoanFlow · временный стенд приёмки"
    values = {f"{name}_token": token for name, token in tokens.items()}
    values.update(base_url=base_url, sms_code=sms_code)
    for item in env["values"]:
        item["value"] = values.get(item["key"], item["value"])
    with open(path, "x", encoding="utf-8", opener=lambda name, flags: os.open(name, flags, 0o600)) as output:
        json.dump(env, output, ensure_ascii=False, indent=2)


def get_json(url, token):
    with urlopen(Request(url, headers={"Authorization": f"Bearer {token}"}), timeout=5) as response:
        return json.load(response)


def check_pdf(base_url, token, expected):
    [card] = get_json(base_url + "/applications", token)
    application_url = f"{base_url}/applications/{card['application_id']}"
    offer = get_json(application_url + "/offer", token)
    url = f"{application_url}/terms/{offer['terms_id']}/document"
    with urlopen(Request(url, headers={"Authorization": f"Bearer {token}"}), timeout=5) as response:
        assert response.headers.get_content_type() == "application/pdf"
        pdf = response.read()
    assert sha256(pdf).hexdigest() == offer["document_sha256"], f"GET {url}. SHA-256 PDF отличается от предложения"
    result = subprocess.run(["pdftotext", "-enc", "UTF-8", "-", "-"], input=pdf, capture_output=True,
                            check=True, timeout=10)
    label, _, _ = expected.partition(":")
    actual = next((line for line in result.stdout.decode().splitlines() if line.startswith(label + ":")), None)
    assert actual == expected, f"GET {url}. Ожидалось {expected!r}, получено {actual!r}"
    print(f"PDF · SHA-256 совпал · {actual}")


def check_database(dsn):
    with connect(dsn) as db:
        draft = one(db, """SELECT application_id FROM application WHERE status='DRAFT'""")
        for table, column in (("submission_snapshot", "application_id"), ("service_job", "resource_id"),
                              ("stand_adapter_call", "resource_id")):
            assert not one(db, f"SELECT 1 FROM {table} WHERE {column}=%s", (draft["application_id"],)), table

        rows = db.execute("""SELECT s.principal, a.application_id, a.status, d.rejection_reason_code,
                                    e.recommendation->>'psk_annual_percent' AS calculated_psk,
                                    r.state AS reserve, t.terms_id IS NOT NULL AS has_terms
                             FROM application a JOIN stand_session s USING (customer_id)
                             LEFT JOIN final_decision d USING (application_id)
                             LEFT JOIN evaluation e USING (application_id)
                             LEFT JOIN offer o USING (application_id)
                             LEFT JOIN mpl_reservation r USING (offer_id)
                             LEFT JOIN individual_terms t USING (offer_id)
                             WHERE a.status <> 'DRAFT'""")
        cases = {row["principal"]: row for row in rows}
        assert cases["refusal"]["rejection_reason_code"] == "NO_CONFIRMED_INCOME", cases["refusal"]
        assert not one(db, "SELECT 1 FROM final_decision WHERE application_id=%s",
                       (cases["error"]["application_id"],)), cases["error"]

        actual_psk = [(cases[name]["calculated_psk"], cases[name]["rejection_reason_code"],
                       cases[name]["reserve"], cases[name]["has_terms"]) for name in BOUNDARY_PAYMENTS]
        assert actual_psk == [
            ("24.990000000000", None, "HELD", True),
            ("25.000000000000", None, "HELD", True),
            ("25.010000000000", "PSK_LIMIT_EXCEEDED", None, False),
        ], actual_psk
        issued = cases["mobile"]
        assert issued["reserve"] == "CONSUMED", issued
        assert one(db, """SELECT count(*) AS n FROM stand_adapter_call WHERE resource_id=%s AND operation='AML'""",
                   (issued["application_id"],))["n"] == 1
        assert one(db, """SELECT count(*) AS n FROM signing_attempt JOIN individual_terms USING (terms_id)
                          WHERE application_id=%s""", (issued["application_id"],))["n"] == 1
    print("PostgreSQL · черновик без оценки, повторы без дублей, внутренние исходы и резервы подтверждены")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--newman", default="newman", help="исполняемый файл Newman")
    parser.add_argument("--postman", action="store_true", help="оставить стенд запущенным для Postman Desktop")
    args = parser.parse_args()
    if not args.postman:
        for executable in (args.newman, "pdftotext"):
            if not shutil.which(executable):
                parser.error(f"Не найден {executable}")

    with TemporaryDirectory(prefix="loanflow-postman-") as tmp, temporary_postgres() as (params, _):
        work = Path(tmp)
        dsn = make_conninfo(**params)
        migrate(dsn)
        scenarios = work / "scenarios.json"
        build_scenarios(scenarios)
        secrets = {"LOANFLOW_SECRET": token_hex(32), "LOANFLOW_ABS_TOKEN": token_urlsafe(32)}
        sms_code = f"{randbelow(1_000_000):06d}"
        with patch.dict(os.environ, secrets):
            tokens = seed(scenarios, dsn)
            with http_server(create_abs(dsn)) as abs_port, http_server(create_app(dsn)) as api_port, \
                    http_server(create_stubs(dsn)) as checks_port, \
                    fixture_worker(dsn, abs_port, checks_port, sms_code):
                base_url = f"http://127.0.0.1:{api_port}"
                environment = work / "local.postman_environment.json"
                prepare_environment(environment, tokens, base_url, sms_code)
                if args.postman:
                    print(f"Окружение Postman · {environment}\nОстановить стенд · Ctrl+C", flush=True)
                    try:
                        Event().wait()
                    except KeyboardInterrupt:
                        return
                else:
                    subprocess.run([args.newman, "run", str(COLLECTION), "-e", str(environment),
                                    "--timeout", "120000", "--timeout-request", "5000",
                                    "--timeout-script", "5000", "--bail", "failure", "--color", "off"],
                                   check=True, timeout=130)
                    check_pdf(base_url, tokens["pdn_at"], "Ежемесячный платёж: 4631.73 ₽")
                    check_pdf(base_url, tokens["pdf"], "ПСК годовых: 20.003%")
                    check_database(dsn)


if __name__ == "__main__":
    main()
