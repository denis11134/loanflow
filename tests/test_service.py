"""Запросы через TCP к FastAPI с отдельным PostgreSQL."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from http.client import HTTPConnection
import json
import hashlib
import socket
import threading
import time
from uuid import uuid4

import psycopg
import pytest
import uvicorn
from reportlab.pdfbase.ttfonts import TTFError

from api_support import document, operations, response_schema, validator
from services.loanflow import applications, assessment, calculations
from services.loanflow.api import create_app
from services.loanflow.check_stubs import create_stubs
from services.loanflow.seed import DEMO, seed
from services.loanflow.signing import stand_code
from services.loanflow.worker import Worker
from services.loanflow.store import connect, migrate
from test_database import temporary_postgres

pytestmark = pytest.mark.service


class Clock:
    at = datetime(2026, 11, 6, 13, 30, tzinfo=timezone.utc)

    def __call__(self):
        return self.at


@contextmanager
def http_server(app):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, log_level="critical", access_log=False))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
        thread.start()
        try:
            until = time.monotonic() + 5
            while not server.started and thread.is_alive() and time.monotonic() < until:
                time.sleep(0.01)
            assert server.started
            yield port
        finally:
            server.should_exit = True
            thread.join(5)
            assert not thread.is_alive()


class Stand:
    def __init__(self, dsn, clock, tokens):
        self.dsn = dsn
        self.clock = clock
        self.tokens = tokens

    def request(self, method, path, code, actor="mobile", body=None, key=None, headers=None):
        params = {"Authorization": f"Bearer {self.tokens[actor]}", **(headers or {})}
        if body is not None:
            params["Content-Type"] = "application/json"
        if key:
            params["Idempotency-Key"] = str(key)
        conn = HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request(method, path, None if body is None else json.dumps(body), params)
            response = conn.getresponse()
            raw = response.read()
            payload = raw if response.headers["Content-Type"] == "application/pdf" else json.loads(raw)
            if isinstance(code, tuple):
                assert response.status in code, (response.status, payload)
            else:
                assert response.status == code, (response.status, payload)
            doc = document("loanflow")
            url = path.split("?")[0].split("/")
            op = next(operation for template, verb, operation in operations(doc)
                      if verb == method.lower() and len(template.split("/")) == len(url)
                      and all(a.startswith("{") or a == b for a, b in zip(template.split("/"), url)))
            if not isinstance(payload, bytes):
                validator(doc, response_schema(doc, op["operationId"], response.status)).validate(payload)
            return payload
        finally:
            conn.close()

    @contextmanager
    def serve(self):
        with http_server(create_app(self.dsn, self.clock)) as port:
            self.port = port
            yield self

    def sql(self, query, params=()):
        with connect(self.dsn) as db:
            cursor = db.execute(query, params)
            return cursor.fetchall() if cursor.description else None

    def draft(self, actor="mobile", amount="5000000", term=12):
        return self.request("POST", "/applications", 201, actor,
                            {"requested_amount_minor": amount, "requested_term_months": term}, uuid4())["application_id"]


@pytest.fixture
def setup_stand(monkeypatch, tmp_path):
    monkeypatch.setenv("LOANFLOW_SECRET", "42" * 32)
    data = json.loads(DEMO.read_text())
    alt = deepcopy(data["customers"]["mobile"])
    alt.update(customer_id="10000000-0000-0000-0000-000000000002",
               account_ref="stand-account-2", profile_ref="stand-profile-2")
    data["customers"]["other"] = alt
    data["actors"]["other"] = {"role": "CUSTOMER", "customer": "other"}
    config = tmp_path / "scenarios.json"
    config.write_text(json.dumps(data))
    with temporary_postgres() as (params, _):
        dsn = psycopg.conninfo.make_conninfo(**params)
        migrate(dsn)
        migrate(dsn)
        stand = Stand(dsn, Clock(), seed(config, dsn))
        with http_server(create_stubs(dsn)) as port:
            monkeypatch.setenv("LOANFLOW_CHECKS_URL", f"http://127.0.0.1:{port}")
            yield stand


def test_submission_http_restart_and_race(setup_stand):
    stand = setup_stand
    key = uuid4()
    with stand.serve():
        draft = stand.request("POST", "/applications", 201, key=key)
        app = draft["application_id"]
        assert draft["requested_amount_minor"] is None
        assert stand.request("POST", "/applications", 201, key=key) == draft
        assert stand.request("POST", "/applications", 409, body={"requested_term_months": 12}, key=key)["code"] == "IDEMPOTENCY_KEY_REUSED"
        assert stand.request("POST", f"/applications/{app}/submission", 422)["code"] == "REQUIRED_FIELDS_MISSING"
        patch = {"requested_amount_minor": "9223372036854775807", "requested_term_months": 12}
        assert stand.request("PATCH", f"/applications/{app}", 200, body=patch, key=uuid4())["requested_amount_minor"] == patch["requested_amount_minor"]
        assert stand.request("POST", f"/applications/{app}/submission", 422)["code"] == "APPLICATION_PARAMETERS_OUT_OF_RANGE"
        assert stand.request("GET", f"/applications/{app}", 200)["status"] == "DRAFT"
        assert not stand.sql("SELECT * FROM submission_snapshot")
        assert not stand.sql("SELECT * FROM service_job")
        stand.request("PATCH", f"/applications/{app}", 200, body={"requested_amount_minor": "100000"}, key=uuid4())
        assert stand.request("POST", "/applications", 400, body={"requested_amount_minor": "9223372036854775808"}, key=uuid4())["code"] == "INVALID_REQUEST"
        assert stand.request("GET", f"/applications/{app}", 404, "other") == stand.request("GET", f"/applications/{uuid4()}", 404, "other")
        assert stand.request("GET", "/applications", 403, "branch")["code"] == "FORBIDDEN"
        second = stand.draft()
        barrier = threading.Barrier(2)

        def submit(app_id):
            barrier.wait(5)
            return stand.request("POST", f"/applications/{app_id}/submission", (202, 409))

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(submit, (app, second)))
        receipt, = [r for r in results if r.get("accepted_status") == "SCORING"]
        blocked, = [r for r in results if r.get("code") == "BLOCKING_APPLICATION_OR_CONTRACT"]
        assert blocked
        assert len(stand.sql("SELECT * FROM submission_snapshot")) == len(stand.sql("SELECT * FROM service_job")) == 1
    with stand.serve():
        accepted = receipt["application_id"]
        assert len(stand.request("GET", "/applications", 200)) == 2
        assert stand.request("GET", f"/applications/{accepted}", 200)["status"] == "SCORING"
        assert stand.request("POST", f"/applications/{accepted}/submission", 202) == receipt
        assert stand.request("POST", f"/applications/{accepted}/cancellation", 200)["status"] == "CLIENT_CANCELED"
        assert stand.request("POST", f"/applications/{accepted}/submission", 202) == receipt
        stand.sql("UPDATE stand_session SET enabled=false WHERE principal='mobile'")
        assert stand.request("POST", "/applications", 403, key=key)["code"] == "FORBIDDEN"


def pause_at(name, entered, proceed):
    def checkpoint(current, *_):
        if current == name:
            entered.set()
            assert proceed.wait(5)
    return checkpoint


class SimulatedProcessStop(BaseException):
    pass


def crash_at(name):
    def checkpoint(current, *_):
        if current == name:
            raise SimulatedProcessStop
    return checkpoint


def worker(stand, **kwargs):
    return Worker(stand.dsn, stand.clock, **kwargs)


def process(stand, *kinds, **kwargs):
    instance = worker(stand, **kwargs)
    for kind in kinds:
        assert instance.run_once(kind), kind
    return instance


def update_scenario(stand, values, actor="mobile"):
    customer = 1 if actor == "mobile" else 2
    stand.sql("UPDATE stand_profile SET scenario=scenario || %s::jsonb WHERE customer_id=%s",
              (json.dumps(values), f"10000000-0000-0000-0000-00000000000{customer}"))


def submit(stand, actor="mobile", **draft):
    app = stand.draft(actor, **draft)
    stand.request("POST", f"/applications/{app}/submission", 202, actor)
    return app


def prepare_offer(stand, amount=None):
    if amount is None:
        app = submit(stand)
    else:
        if Decimal(amount) >= 200_000:
            update_scenario(stand, {"payroll_income_rub": "80000.00"})
        app = submit(stand, amount=applications.minor(Decimal(amount)))
    process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE")
    assert stand.request("GET", f"/applications/{app}/offer", 200)["state"] == "PREPARING"
    process(stand, "PUBLISH")
    path = f"/applications/{app}/offer"
    offer = stand.request("GET", path, 200)
    pdf = stand.request("GET", f"/applications/{app}/terms/{offer['terms_id']}/document", 200)
    assert pdf.startswith(b"%PDF-") and hashlib.sha256(pdf).hexdigest() == offer["document_sha256"]
    assert stand.request("GET", path, 200) == offer
    return app, offer


def start_attempt(stand, app, offer):
    path = f"/applications/{app}/signing-attempts"
    assert stand.request("POST", path, 409, key=uuid4())["code"] == "PDN_NOTICE_NOT_ACKNOWLEDGED"
    ack = f"/applications/{app}/terms/{offer['terms_id']}/pdn-notice-acknowledgement"
    version = {"notice_version": offer["pdn_notice"]["version"]}
    result = stand.request("PUT", ack, 200, body=version)
    assert stand.request("PUT", ack, 200, body=version) == result
    key = uuid4()
    attempt = stand.request("POST", path, 201, key=key)
    assert stand.request("POST", path, 201, key=key) == attempt
    process(stand, "SMS")
    return path + "/" + attempt["attempt_id"], attempt


def verify(stand, attempt_path, attempt, key=None, code=None):
    return stand.request("POST", attempt_path + "/verification", 202,
                         body={"code": code or stand_code(attempt["attempt_id"])}, key=key or uuid4())


def signed_contract(stand, amount=None):
    app, offer = prepare_offer(stand, amount)
    path, attempt = start_attempt(stand, app, offer)
    verify(stand, path, attempt)
    process(stand, "VERIFY")
    signed = stand.request("GET", path, 200)
    assert signed["state"] == "SIGNED"
    return signed["contract_id"]


def assert_reserve(stand, state):
    assert stand.sql("SELECT state FROM mpl_reservation") == [{"state": state}]


def test_worker_recovery_retries_and_late_assessment(setup_stand):
    stand = setup_stand
    with stand.serve():
        update_scenario(stand, {"faults": {"AML": ["TIMEOUT", "TIMEOUT", "TIMEOUT"]}})
        first = submit(stand)
    with stand.serve():
        process(stand, "AML")
        assert stand.sql("SELECT failures,run_at FROM service_job") == [
            {"failures": 1, "run_at": stand.clock() + timedelta(seconds=1)}]
        assert not worker(stand).run_once("AML")
        stand.clock.at += timedelta(seconds=1)
        process(stand, "AML")
        stand.clock.at += timedelta(seconds=3)
        process(stand, "AML")
        assert stand.request("GET", f"/applications/{first}", 200)["status"] == "SYSTEM_ERROR"
        assert stand.sql("SELECT operation FROM stand_adapter_call") == [{"operation": "AML"}] * 3
        update_scenario(stand, {"faults": {}})
        app = submit(stand)
        entered, proceed = threading.Event(), threading.Event()
        with ThreadPoolExecutor() as pool:
            pending = pool.submit(worker(stand, checkpoint=pause_at("after_aml", entered, proceed)).run_once, "AML")
            assert entered.wait(5)
            stand.request("POST", f"/applications/{app}/cancellation", 200)
            proceed.set()
            assert pending.result(5)
        assert stand.request("GET", f"/applications/{app}", 200)["status"] == "CLIENT_CANCELED"
        assert not stand.sql("SELECT * FROM final_decision")
        assert not stand.sql("SELECT * FROM service_job")


@pytest.mark.parametrize(("patch", "reason", "operations"), [
    ({"aml_block": True, "self_ban": True}, "REFUSED", ["AML"]),
    ({"consent_ref": None}, "CREDIT_REPORT_CONSENT_REQUIRED", ["AML", "PASSPORT"]),
    ({"passport_verdict": "INVALID", "consent_ref": None}, "REFUSED", ["AML", "PASSPORT"]),
    ({"self_ban": True}, "SELF_BAN", ["AML", "PASSPORT", "CREDIT_REPORT"]),
    ({"payroll_income_rub": "40000.00"}, "REFUSED", ["AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE"]),
])
def test_assessment_stop_factors(setup_stand, patch, reason, operations):
    stand = setup_stand
    with stand.serve():
        update_scenario(stand, patch)
        app = submit(stand)
        process(stand, *operations)
        assert stand.request("GET", f"/applications/{app}", 200)["display_reason"] == reason
        assert [r["operation"] for r in stand.sql("SELECT operation FROM stand_adapter_call ORDER BY call_id")] == operations
        if patch.get("passport_verdict") == "INVALID":
            assert stand.sql("SELECT rejection_reason_code FROM final_decision") == [
                {"rejection_reason_code": "PASSPORT_INVALID"}]
        if operations[-1] == "DE":
            assert stand.sql("SELECT rejection_reason_code FROM final_decision") == [
                {"rejection_reason_code": "SCORING_BELOW_THRESHOLD"}]


@pytest.mark.parametrize("legacy", [False, True], ids=["current", "legacy"])
def test_seed_rejects_missing_quota_without_partial_load(setup_stand, tmp_path, legacy):
    stand = setup_stand
    data = json.loads(DEMO.read_text())
    valid = data["customers"]["mobile"]
    valid.update(customer_id=str(uuid4()), profile_ref="new-profile")
    missing_quota = {"quota_period": "missing-period", "quota_bucket": "missing-bucket"}
    data["customers"]["invalid"] = {
        **valid, "customer_id": str(uuid4()), "profile_ref": "invalid-profile",
        "scenario": {"offer": missing_quota} if legacy else missing_quota,
    }
    data["holidays"] = ["2026-11-12"]
    data["quotas"] = [{"period": "new-period", "bucket": "standard", "capacity": 1}]
    config = tmp_path / "invalid-scenarios.json"
    config.write_text(json.dumps(data))

    tables = ("customer", "stand_profile", "stand_session", "stand_holiday", "mpl_quota")
    before = [stand.sql(f"SELECT count(*) FROM {table}") for table in tables]
    with pytest.raises(ValueError, match="missing-period.*missing-bucket"):
        seed(config, stand.dsn)
    assert [stand.sql(f"SELECT count(*) FROM {table}") for table in tables] == before


@pytest.mark.parametrize("manual", [False, True], ids=["automatic", "manual"])
def test_missing_quota_is_application_system_error(setup_stand, manual):
    stand = setup_stand
    with stand.serve():
        stand.sql("INSERT INTO mpl_quota VALUES (%s,'removed','standard',1)", (uuid4(),))
        update_scenario(stand, {"quota_period": "removed"})
        app = submit(stand, term=37 if manual else 12)
        other = submit(stand, "other")
        process(stand, "AML", "AML", "PASSPORT", "PASSPORT", "CREDIT_REPORT", "CREDIT_REPORT",
                "FRAUD", "FRAUD")
        if manual:
            process(stand, "DE")
            assert stand.request("GET", f"/applications/{app}", 200)["status"] == "UNDERWRITING"
        stand.sql("DELETE FROM mpl_quota WHERE period_key='removed'")
        if manual:
            path = f"/underwriting/applications/{app}/decision"
            key = uuid4()
            body = {"command": "APPROVE"}
            response = stand.request("POST", path, 503, "underwriter", body, key)
            assert response["code"] == "SERVICE_UNAVAILABLE"
            assert stand.request("POST", path, 503, "underwriter", body, key) == response
            assert stand.sql("SELECT underwriting_exited_at FROM application WHERE application_id=%s", (app,)) == [
                {"underwriting_exited_at": stand.clock()}]
        else:
            process(stand, "DE")
        process(stand, "DE", "PUBLISH")
        assert stand.request("GET", f"/applications/{app}", 200)["status"] == "SYSTEM_ERROR"
        assert stand.request("GET", f"/applications/{other}", 200, "other")["status"] == "APPROVED"
        assert not stand.sql("SELECT 1 FROM final_decision WHERE application_id=%s", (app,))
        assert not stand.sql("SELECT 1 FROM offer WHERE application_id=%s", (app,))


def test_exhausted_quota_is_bank_rejection(setup_stand):
    stand = setup_stand
    with stand.serve():
        stand.sql("UPDATE mpl_quota SET capacity=0")
        app = submit(stand)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE")
        assert stand.request("GET", f"/applications/{app}", 200)["status"] == "BANK_REJECTED"
        assert stand.sql("SELECT rejection_reason_code FROM final_decision WHERE application_id=%s", (app,)) == [
            {"rejection_reason_code": "MPL_QUOTA_EXHAUSTED"}]
        assert not stand.sql("SELECT 1 FROM mpl_reservation")


def test_underwriting_last_quota_sla_and_weekly_report(setup_stand):
    stand = setup_stand
    with stand.serve():
        stand.sql("UPDATE mpl_quota SET capacity=1")
        apps = {actor: submit(stand, actor, term=37) for actor in ("mobile", "other")}
        process(stand, "AML", "AML", "PASSPORT", "PASSPORT", "CREDIT_REPORT", "CREDIT_REPORT",
                "FRAUD", "FRAUD", "DE", "DE")
        cards = stand.request("GET", "/underwriting/applications", 200, "underwriter")
        assert len(cards) == 2
        assert all(card["sla_deadline_at"] == "2026-11-09T05:30:00+00:00" for card in cards)
        for app in apps.values():
            assert stand.request("GET", f"/underwriting/applications/{app}", 200, "underwriter") in cards
        prepared = stand.sql("SELECT recommendation FROM evaluation")
        assert prepared[0] == prepared[1]
        conditions = prepared[0]["recommendation"]
        for actor in ("mobile", "other"):
            update_scenario(stand, {"quota_period": "unexpected", "payroll_income_rub": "0"}, actor)
        stand.clock.at = datetime(2026, 11, 9, 5, 30, 1, tzinfo=timezone.utc)

        def approve(app):
            path = f"/underwriting/applications/{app}/decision"
            key = uuid4()
            body = {"command": "APPROVE"}
            result = stand.request("POST", path, 200, "underwriter", body, key)
            assert stand.request("POST", path, 200, "underwriter", body, key) == result
            return result

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(approve, apps.values()))
        assert sorted(r["decision"] for r in results) == ["APPROVED", "REJECTED"]
        assert next(r for r in results if r["decision"] == "REJECTED")["rejection_reason_code"] == "MPL_QUOTA_EXHAUSTED"
        approved, = stand.sql("SELECT * FROM offer")
        assert approved["term_months"] == conditions["term_months"] == 37
        for field in ("amount_rub", "annual_rate_percent", "monthly_payment_rub", "psk_annual_percent", "psk_rub"):
            assert approved[field] == Decimal(conditions[field])
        assert_reserve(stand, "HELD")
        assert stand.request("GET", "/underwriting/escalations", 200, "supervisor") == []
        process(stand, "SLA", "SLA")
        assert len(stand.request("GET", "/underwriting/escalations", 200, "supervisor")) == 2
        report = stand.request("GET", "/underwriting/reports/weekly?week_start=2026-11-09&as_of=2026-11-16T00:00:00%2B04:00",
                               200, "supervisor")["underwriters"][0]
        assert report["decision_count"] == 2 and report["approval_rate"] == 0.5 and report["overdue_rate"] is None


@pytest.mark.parametrize(("amount", "term", "accepted"), [
    ("99999", 1, False), ("100000", 1, True), ("100000000", 60, True),
    ("100000001", 60, False), ("100000", 61, False),
])
def test_product_range_is_checked_on_submission(setup_stand, amount, term, accepted):
    stand = setup_stand
    with stand.serve():
        app = stand.draft(amount=amount, term=term)
        path = f"/applications/{app}"
        receipt = stand.request("POST", path + "/submission", 202 if accepted else 422)
        if accepted:
            assert stand.sql("SELECT rules_version FROM submission_snapshot") == [{"rules_version": "3.3"}]
            assert stand.request("POST", path + "/submission", 202) == receipt
        else:
            assert receipt["code"] == "APPLICATION_PARAMETERS_OUT_OF_RANGE"
            assert stand.request("GET", path, 200)["status"] == "DRAFT"
            assert not stand.sql("SELECT * FROM submission_snapshot")
            assert not stand.sql("SELECT * FROM service_job")


def test_calculation_migration_requires_empty_assessment_queue(setup_stand):
    stand = setup_stand
    with stand.serve():
        approved, offer = prepare_offer(stand)
        doc_path = f"/applications/{approved}/terms/{offer['terms_id']}/document"
        pdf = stand.request("GET", doc_path, 200)
        pending = submit(stand, actor="other")

        # Восстанавливаем схему до миграции 007, сохраняя заявки и ИУ.
        stand.sql("ALTER TABLE submission_snapshot DROP COLUMN rules_version")
        stand.sql("DELETE FROM service_migration WHERE name='007_calculation_rules.sql'")
        with pytest.raises(psycopg.errors.RaiseException, match="Complete existing SCORING and UNDERWRITING"):
            migrate(stand.dsn)
        assert stand.request("POST", f"/applications/{pending}/cancellation", 200, "other")
        migrate(stand.dsn)

        assert stand.request("GET", f"/applications/{approved}/offer", 200) == offer
        assert stand.request("GET", doc_path, 200) == pdf


@pytest.mark.parametrize(("income", "remove_payments", "status", "reason"), [
    (None, True, "SYSTEM_ERROR", None),
    (None, False, "BANK_REJECTED", "NO_CONFIRMED_INCOME"),
    (0, False, "BANK_REJECTED", "NO_CONFIRMED_INCOME"),
    ("-0.01", False, "SYSTEM_ERROR", None),
])
def test_pdn_source_validation_precedes_no_income(setup_stand, income, remove_payments, status, reason):
    stand = setup_stand
    with stand.serve():
        update_scenario(stand, {"payroll_income_rub": income})
        if remove_payments:
            stand.sql("UPDATE stand_profile SET scenario=scenario - 'existing_payments_rub'")
        app = submit(stand)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT")
        view = stand.request("GET", f"/applications/{app}", 200)
        assert view["status"] == status
        assert view["display_reason"] == ("REFUSED" if reason else None)
        assert stand.sql("SELECT rejection_reason_code FROM final_decision") == (
            [{"rejection_reason_code": reason}] if reason else [])
        assert not stand.sql("SELECT * FROM service_job")
        assert stand.sql("SELECT operation FROM stand_adapter_call ORDER BY call_id") == [
            {"operation": kind} for kind in ("AML", "PASSPORT", "CREDIT_REPORT")]


@pytest.mark.parametrize(("payments", "required"), [("45368.27", False), ("45378.27", True)])
def test_pdn_notice_uses_exact_ratio(setup_stand, payments, required):
    stand = setup_stand
    with stand.serve():
        update_scenario(stand, {"payroll_income_rub": "100000.00", "existing_payments_rub": payments})
        app, offer = prepare_offer(stand)
        assert offer["monthly_payment_minor"] == "463173"
        assert offer["psk_annual_percent"] == "20.000006146107"
        assert offer["psk_rub"] == "5580.70"
        assert offer["pdn_notice_required"] is required
        assert ("pdn_notice" in offer) is required
        card = stand.request("GET", f"/applications/{app}", 200)
        assert ("START_SIGNING" in card["available_actions"]) is not required


@pytest.mark.parametrize(("payment", "psk", "approved"), [
    (122499, "24.990000000000", True), (122500, "25.000000000000", True), (122501, "25.010000000000", False),
])
def test_psk_and_publication_recovery(setup_stand, monkeypatch, payment, psk, approved):
    stand = setup_stand
    def limit_schedule(principal, term, rate):
        assert (principal, term, rate) == (120000, 1, calculations.MONTHLY_RATE)
        return payment, [payment], payment - principal

    # Эти потоки проверяют предел отдельно от тарифа 20% учебного продукта.
    monkeypatch.setattr(calculations, "annuity_schedule", limit_schedule)
    with stand.serve():
        app = submit(stand, amount="120000", term=1)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE")
        assert stand.sql("SELECT recommendation->>'psk_annual_percent' AS psk FROM evaluation") == [{"psk": psk}]
    with stand.serve():
        if not approved:
            assert not stand.sql("SELECT * FROM mpl_reservation")
            assert not stand.sql("SELECT * FROM individual_terms")
            assert stand.sql("SELECT rejection_reason_code FROM final_decision") == [
                {"rejection_reason_code": "PSK_LIMIT_EXCEEDED"}]
            assert stand.request("GET", f"/applications/{app}/offer", 409)["code"] == "OFFER_NOT_AVAILABLE"
            return
        assert stand.request("GET", f"/applications/{app}/offer", 200)["state"] == "PREPARING"
        with pytest.raises(SimulatedProcessStop):
            worker(stand, checkpoint=crash_at("after_publish")).run_once("PUBLISH")
        assert not stand.sql("SELECT * FROM individual_terms")
        process(stand, "PUBLISH")
        offer = stand.request("GET", f"/applications/{app}/offer", 200)
        assert offer["offer_expires_at"] == "2026-11-13T13:30:00+00:00"
        assert_reserve(stand, "HELD")


def test_signing_retry_and_contact_center_replay(setup_stand):
    stand = setup_stand
    with stand.serve():
        app, offer = prepare_offer(stand)
        path, attempt = start_attempt(stand, app, offer)
        sms = path + "/sms-resends"
        key = uuid4()
        first = stand.request("POST", sms, 202, key=key)
        assert stand.request("POST", sms, 202, key=key) == first
        assert stand.request("POST", sms, 202, key=uuid4())["resend_number"] == 2
        assert stand.request("POST", sms, 409, key=uuid4())["code"] == "SMS_RESEND_LIMIT"
        process(stand, "SMS", "SMS")
        assert stand.sql("SELECT count(*) AS n,count(DISTINCT verification_digest) AS codes FROM stand_sms_delivery") == [
            {"n": 3, "codes": 1}]
        key = uuid4()
        accepted = verify(stand, path, attempt, key)
        assert stand.request("GET", path, 200)["state"] == "VERIFYING"
        assert stand.request("POST", sms, 409, key=uuid4())["code"] == "VERIFICATION_IN_PROGRESS"
        assert verify(stand, path, attempt, key) == accepted
    with stand.serve():
        process(stand, "VERIFY")
        contract = stand.request("GET", path, 200)["contract_id"]
        assert stand.request("GET", f"/contracts/{contract}", 200)["status"] == "COOLING_OFF"
        assert stand.request("GET", f"/contracts/{contract}", 404, "other")["code"] == "NOT_FOUND"
        customer_id = "10000000-0000-0000-0000-000000000001"
        status = stand.request("GET", f"/contact-center/customers/{customer_id}/status", 200, "contact")
        assert status["contracts"][0]["withdrawal_available_after_identification"]
        assert stand.request("GET", f"/branches/customers/{customer_id}/presence", 200, "branch") == {
            "has_active_application": False, "has_active_contract": True}
        contact = f"/contact-center/contracts/{contract}"
        token = stand.request("POST", contact + "/identification", 200, "contact")["identification_token"]
        headers = {"X-Identification-Token": token}
        key = uuid4()
        cancel = stand.request("POST", contact + "/withdrawal", 200, "contact", key=key, headers=headers)
        stand.clock.at += timedelta(minutes=6)
        assert stand.request("POST", contact + "/withdrawal", 200, "contact", key=key, headers=headers) == cancel
        assert_reserve(stand, "RELEASED")
        assert not stand.sql("SELECT * FROM disbursement_outbox")
        journals = stand.sql("SELECT fingerprint,response_body FROM accepted_request")
        for row in journals:
            for secret in (token, stand.tokens["mobile"], stand_code(attempt["attempt_id"]), "5000000"):
                assert all(secret.encode() not in bytes(value) for value in row.values())


@pytest.mark.parametrize("verification_first", [True, False])
def test_verification_and_timer_race(setup_stand, verification_first):
    stand = setup_stand
    with stand.serve():
        app, offer = prepare_offer(stand)
        path, attempt = start_attempt(stand, app, offer)
        verify(stand, path, attempt)
        entered, proceed = threading.Event(), threading.Event()
        checkpoint = "before_sign_commit" if verification_first else "after_verify"
        with ThreadPoolExecutor(max_workers=2) as pool:
            pep = pool.submit(worker(stand, checkpoint=pause_at(checkpoint, entered, proceed)).run_once, "VERIFY")
            assert entered.wait(5)
            stand.clock.at = datetime.fromisoformat(attempt["expires_at"])
            timer = pool.submit(worker(stand).run_once, "ATTEMPT_EXPIRY")
            if not verification_first:
                assert timer.result(5)
            proceed.set()
            assert pep.result(5) and timer.result(5)
        view = stand.request("GET", path, 200)
        if verification_first:
            assert view["state"] == "SIGNED"
            assert len(stand.sql("SELECT * FROM contract")) == 1
        else:
            assert view["close_reason"] == "ATTEMPT_EXPIRED"
            assert not stand.sql("SELECT * FROM signature_evidence")


@pytest.mark.parametrize("launch_first", [True, False])
def test_withdrawal_against_cooling_job(setup_stand, launch_first):
    stand = setup_stand
    with stand.serve():
        contract = signed_contract(stand, "200000.00")
        path = f"/contracts/{contract}"
        view = stand.request("GET", path, 200)
        assert datetime.fromisoformat(view["cooling_ends_at"]) - stand.clock() == timedelta(hours=4)
        assert not worker(stand).run_once("COOLING")
        stand.clock.at = datetime.fromisoformat(view["cooling_ends_at"])
        if launch_first:
            entered, proceed = threading.Event(), threading.Event()
            with ThreadPoolExecutor(max_workers=2) as pool:
                launch = pool.submit(worker(stand, checkpoint=pause_at("before_cooling_commit", entered, proceed))
                                     .run_once, "COOLING")
                assert entered.wait(5)
                withdrawal = pool.submit(stand.request, "POST", path + "/withdrawal", 409)
                proceed.set()
                assert launch.result(5)
                assert withdrawal.result(5)["code"] == "WITHDRAWAL_UNAVAILABLE"
            assert len(stand.sql("SELECT * FROM disbursement_outbox")) == 1
            assert_reserve(stand, "HELD")
        else:
            stand.request("POST", path + "/withdrawal", 200)
            process(stand, "COOLING")
            assert not stand.sql("SELECT * FROM disbursement_outbox")
            assert_reserve(stand, "RELEASED")


@pytest.mark.parametrize(("faults", "steps"), [(["PERMANENT"], 1), (["TIMEOUT"] * 3, 3)])
def test_failed_publication_releases_reserve(setup_stand, faults, steps):
    stand = setup_stand
    with stand.serve():
        update_scenario(stand, {"faults": {"PUBLISH": faults}})
        app = submit(stand)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE")
        for n in range(steps):
            process(stand, "PUBLISH")
            stand.clock.at += timedelta(seconds=1 if n == 0 else 3)
        assert stand.request("GET", f"/applications/{app}", 200)["status"] == "SYSTEM_ERROR"
        assert_reserve(stand, "RELEASED")
        assert not stand.sql("SELECT * FROM individual_terms")


@pytest.mark.parametrize(("errors", "status"), [
    pytest.param([TTFError], "SYSTEM_ERROR", id="permanent"),
    pytest.param([TimeoutError] * 2, "APPROVED", id="recovered"),
    pytest.param([TimeoutError] * 3, "SYSTEM_ERROR", id="exhausted"),
])
def test_publication_handles_generator_exceptions(setup_stand, monkeypatch, errors, status):
    stand = setup_stand
    font = assessment.terms_font
    outcomes = iter(errors)
    retry = errors[0] is TimeoutError

    def generate_font():
        error = next(outcomes, None)
        if error:
            raise error("synthetic rendering failure")
        return font()

    monkeypatch.setattr(assessment, "terms_font", generate_font)
    with stand.serve():
        app = submit(stand)
        path = f"/applications/{app}"
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE")
        assert stand.request("GET", path, 200)["offer_availability"] == "PREPARING"
        assert_reserve(stand, "HELD")

        if retry:
            for count, delay in enumerate((1, 3), start=1):
                process(stand, "PUBLISH")
                next_at = stand.clock() + timedelta(seconds=delay)
                assert stand.sql("SELECT failures,run_at FROM service_job WHERE kind='PUBLISH'") == [
                    {"failures": count, "run_at": next_at}]
                assert stand.request("GET", path, 200)["offer_availability"] == "PREPARING"
                assert_reserve(stand, "HELD")
                assert not worker(stand).run_once("PUBLISH")
                stand.clock.at = next_at

        process(stand, "PUBLISH")
        assert stand.request("GET", path, 200)["status"] == status
        assert not stand.sql("SELECT 1 FROM service_job WHERE kind='PUBLISH'")
        if status == "SYSTEM_ERROR":
            assert_reserve(stand, "RELEASED")
            assert not stand.sql("SELECT 1 FROM individual_terms")
        else:
            assert_reserve(stand, "HELD")
            assert stand.request("GET", path + "/offer", 200)["state"] == "AVAILABLE"
        assert stand.sql("""SELECT sequence FROM stand_adapter_call
                            WHERE operation='PUBLISH' ORDER BY sequence""") == [
            {"sequence": number} for number in ([1, 2, 3] if retry else [1])]


@pytest.mark.parametrize("single", [True, False], ids=["card", "list"])
def test_application_read_during_signing_transition(setup_stand, monkeypatch, single):
    stand = setup_stand
    entered, proceed = threading.Event(), threading.Event()
    pause = pause_at("application_read", entered, proceed)
    original = applications.view

    def paused_view(ctx, row):
        if row["status"] == "APPROVED":
            pause("application_read")
        return original(ctx, row)

    with stand.serve():
        app, offer = prepare_offer(stand)
        path = f"/applications/{app}"
        stand.request("PUT", f"{path}/terms/{offer['terms_id']}/pdn-notice-acknowledgement",
                      200, body={"notice_version": offer["pdn_notice"]["version"]})
        read_path = path if single else "/applications"
        before = stand.request("GET", read_path, 200)
        approved = before if single else before[0]
        assert approved["status"] == "APPROVED" and approved["active_attempt_id"] is None
        assert "START_SIGNING" in approved["available_actions"]

        monkeypatch.setattr(applications, "view", paused_view)
        with ThreadPoolExecutor() as pool:
            reading = pool.submit(stand.request, "GET", read_path, 200)
            try:
                assert entered.wait(5)
                attempt = stand.request("POST", path + "/signing-attempts", 201, key=uuid4())
            finally:
                proceed.set()
            assert reading.result(5) == before

        after = stand.request("GET", read_path, 200)
        signing = after if single else after[0]
        assert signing["status"] == "SIGNING_IN_PROGRESS"
        assert signing["active_attempt_id"] == attempt["attempt_id"]
        assert "START_SIGNING" not in signing["available_actions"]


@pytest.mark.parametrize("offset", [0, 1])
def test_equal_deadlines_and_expired_verification_on_restart(setup_stand, offset):
    stand = setup_stand
    with stand.serve():
        app, offer = prepare_offer(stand)
        stand.clock.at = datetime.fromisoformat(offer["offer_expires_at"]) - timedelta(minutes=5)
        path, attempt = start_attempt(stand, app, offer)
        assert attempt["expires_at"] == attempt["offer_expires_at"]
        verify(stand, path, attempt)
        with pytest.raises(SimulatedProcessStop):
            worker(stand, checkpoint=crash_at("before_sign_commit")).run_once("VERIFY")
        assert not stand.sql("SELECT * FROM contract")
        stand.clock.at += timedelta(minutes=5, seconds=offset)
    with stand.serve():
        process(stand, "VERIFY", "OFFER_EXPIRY", "ATTEMPT_EXPIRY")
        assert stand.request("GET", path, 200)["close_reason"] == "OFFER_EXPIRED"
        assert stand.request("POST", f"/applications/{app}/signing-attempts", 409, key=uuid4())["code"] == "OFFER_EXPIRED"
        assert_reserve(stand, "RELEASED")
        assert not stand.sql("SELECT * FROM signature_evidence")


def test_late_verification_cannot_sign_new_attempt(setup_stand):
    stand = setup_stand
    with stand.serve():
        app, offer = prepare_offer(stand)
        path, attempt = start_attempt(stand, app, offer)
        verify(stand, path, attempt)
        entered, proceed = threading.Event(), threading.Event()
        with ThreadPoolExecutor() as pool:
            pending = pool.submit(worker(stand, checkpoint=pause_at("after_verify", entered, proceed)).run_once, "VERIFY")
            assert entered.wait(5)
            stand.clock.at = datetime.fromisoformat(attempt["expires_at"])
            assert stand.request("POST", path + "/sms-resends", 409, key=uuid4())["code"] == "SIGNING_ATTEMPT_EXPIRED"
            process(stand, "ATTEMPT_EXPIRY")
            base = f"/applications/{app}/signing-attempts"
            fresh = stand.request("POST", base, 201, key=uuid4())
            process(stand, "SMS")
            proceed.set()
            assert pending.result(5)
        assert not stand.sql("SELECT * FROM signature_evidence")
        assert stand.request("GET", path, 200)["close_reason"] == "ATTEMPT_EXPIRED"
        fresh_path = f"{base}/{fresh['attempt_id']}"
        assert stand.request("GET", fresh_path, 200)["state"] == "OPEN"
        verify(stand, fresh_path, fresh, code="invalid")
        process(stand, "VERIFY")
        assert stand.request("GET", fresh_path, 200)["close_reason"] == "INVALID_CODE"
        assert_reserve(stand, "HELD")


def test_long_cooling_and_contact_identification_binding(setup_stand):
    stand = setup_stand
    with stand.serve():
        contract = signed_contract(stand, "200000.01")
        view = stand.request("GET", f"/contracts/{contract}", 200)
        assert datetime.fromisoformat(view["cooling_ends_at"]) - stand.clock() == timedelta(hours=48)
        path = f"/contact-center/contracts/{contract}"
        token = stand.request("POST", path + "/identification", 200, "contact")["identification_token"]
        headers = {"X-Identification-Token": token}
        stand.clock.at += timedelta(minutes=5)
        assert stand.request("POST", path + "/withdrawal", 409, "contact", key=uuid4(), headers=headers)["code"] == "IDENTIFICATION_EXPIRED"
        stand.sql("UPDATE stand_session SET session_ref=%s WHERE principal='contact'", (uuid4(),))
        assert stand.request("POST", path + "/withdrawal", 409, "contact", key=uuid4(), headers=headers)["code"] == "IDENTIFICATION_REQUIRED"
        assert_reserve(stand, "HELD")


def test_legacy_blocks_submit_and_no_protected_input_in_errors(setup_stand, caplog):
    stand = setup_stand
    protected = ["passport-1709", "card-0943", "payroll-9942", "code-1947", "private-fio", "credit-report-2891"]
    with stand.serve():
        app = stand.draft()
        customer_id = "10000000-0000-0000-0000-000000000001"
        stand.sql("INSERT INTO external_contract_ref VALUES ('stand-legacy',%s,true)", (customer_id,))
        assert stand.request("GET", f"/branches/customers/{customer_id}/presence", 200, "branch") == {
            "has_active_application": False, "has_active_contract": True}
        assert stand.request("POST", f"/applications/{app}/submission", 409)["code"] == "BLOCKING_APPLICATION_OR_CONTRACT"
        invalid = stand.request("POST", "/applications", 400, body={"private": protected}, key=uuid4())
        assert not stand.sql("SELECT * FROM submission_snapshot")
        for value in [*protected, stand.tokens["mobile"]]:
            assert value not in caplog.text and value not in json.dumps(invalid)


def test_demo_command(setup_stand, tmp_path):
    import subprocess
    import sys

    stand = setup_stand
    tokens = tmp_path / "tokens.json"
    tokens.write_text(json.dumps(stand.tokens))
    stop = threading.Event()
    instance = worker(stand)

    def run():
        while not stop.is_set():
            if not instance.run_once():
                stop.wait(0.05)

    with stand.serve(), ThreadPoolExecutor() as pool:
        running = pool.submit(run)
        try:
            result = subprocess.run([sys.executable, "scripts/demo_service.py", str(tokens),
                                     "--url", f"http://127.0.0.1:{stand.port}"],
                                    capture_output=True, text=True, timeout=25, check=True)
        finally:
            stop.set()
        running.result(5)
    assert "DRAFT → SCORING → APPROVED" in result.stdout
    assert "SHA-256 PDF подтверждён" in result.stdout
    assert_reserve(stand, "HELD")


def test_submitting_during_contract_creation_uses_same_customer_lock(setup_stand):
    stand = setup_stand
    with stand.serve():
        app, offer = prepare_offer(stand)
        draft = stand.draft()
        path, attempt = start_attempt(stand, app, offer)
        verify(stand, path, attempt)
        entered, proceed = threading.Event(), threading.Event()
        with ThreadPoolExecutor(max_workers=2) as pool:
            signed = pool.submit(worker(stand, checkpoint=pause_at("before_sign_commit", entered, proceed))
                                 .run_once, "VERIFY")
            assert entered.wait(5)
            submitted = pool.submit(stand.request, "POST", f"/applications/{draft}/submission", 409)
            proceed.set()
            assert signed.result(5)
            assert submitted.result(5)["code"] == "BLOCKING_APPLICATION_OR_CONTRACT"
        assert stand.request("GET", f"/applications/{draft}", 200)["status"] == "DRAFT"
        assert len(stand.sql("SELECT * FROM submission_snapshot")) == 1
