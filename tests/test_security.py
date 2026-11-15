"""Регрессии главы 4.5 для учебных HTTP-сервисов и хранилища."""
import json
import os
from datetime import timedelta
from http.client import HTTPConnection
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from api_support import document, operations
from services.loanflow import applications, check_stubs, seed, store
from services.loanflow.abs import create_abs
from services.loanflow.checks import SOAP_ACTION, SOAP_TYPE, passport_message
from services.loanflow.worker import AbsClient
from test_database import temporary_postgres
from test_service import (assert_reserve, http_server, prepare_offer, process, setup_stand, signed_contract,
                          stand_code, start_attempt, submit, verify, worker)
from test_service_abs import abs_server, instruction_id, register

pytestmark = pytest.mark.service
CUSTOMER = "10000000-0000-0000-0000-000000000001"
OTHER_CUSTOMER = "10000000-0000-0000-0000-000000000002"
ACTORS = {
    "mobile": "CUSTOMER",
    "contact": "CONTACT_CENTER",
    "branch": "BRANCH",
    "underwriter": "UNDERWRITER",
    "supervisor": "UNDERWRITING_SUPERVISOR",
    "reconciliation": "RECONCILIATION_OPERATOR",
}


def http_request(port, method, path, headers=None, body=None):
    connection = HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request(method, path, body, headers or {})
        response = connection.getresponse()
        return response.status, response.headers, response.read()
    finally:
        connection.close()


def test_bearer_and_role_matrix(setup_stand, monkeypatch):
    stand = setup_stand
    monkeypatch.setenv("LOANFLOW_ABS_TOKEN", "synthetic-service-credential")
    roles = {
        "CUSTOMER": (
            "createDraft listOwnApplications getOwnApplication updateDraft submitApplication cancelApplication "
            "getOffer getIndividualTermsDocument acknowledgePdnNotice startSigningAttempt getSigningAttempt "
            "resendSigningSms verifySigningCode getOwnContract withdrawInMobile"
        ).split(),
        "CONTACT_CENTER": ["getContactCenterStatus", "identifyContactCenterCustomer", "withdrawInContactCenter"],
        "BRANCH": ["checkBranchCustomerPresence"],
        "UNDERWRITER": ["listUnderwritingQueue", "getUnderwritingApplication", "recordManualDecision"],
        "UNDERWRITING_SUPERVISOR": ["listUnderwritingEscalations", "getWeeklyUnderwritingReport"],
        "RECONCILIATION_OPERATOR": (
            "listReconciliationQueue getReconciliation registerAbsRead getAbsReadResult requestAbsClosure"
        ).split(),
    }
    expected_roles = {operation: role for role, names in roles.items() for operation in names}
    bodies = {
        "updateDraft": {"requested_term_months": 12},
        "acknowledgePdnNotice": {"notice_version": "synthetic"},
        "verifySigningCode": {"code": "714986"},
        "recordManualDecision": {"command": "REJECT"},
    }
    missing = str(uuid4())
    path_parameters = dict.fromkeys(("application_id", "contract_id", "terms_id", "attempt_id", "instruction_id"), missing)
    path_parameters.update(customer_id=CUSTOMER, read_seq=1)
    ops = list(operations(document("loanflow")))
    assert {op["operationId"] for _, _, op in ops} == expected_roles.keys()
    with stand.serve():
        for template, method, op in ops:
            operation = op["operationId"]
            role = expected_roles[operation]
            assert op["x-roles"] == [role]
            path = template.format(**path_parameters)
            if operation == "getWeeklyUnderwritingReport":
                path += "?week_start=2026-11-09&as_of=2026-11-16T00:00:00%2B04:00"
            headers = {"Idempotency-Key": str(uuid4()), "X-Identification-Token": "synthetic-confirmation"}
            body = json.dumps(bodies[operation]) if operation in bodies else None
            if body is not None:
                headers["Content-Type"] = "application/json"
            for auth in ({}, {"Authorization": "Basic synthetic"}, {"Authorization": "Bearer synthetic-unknown"}):
                status, response_headers, raw = http_request(stand.port, method.upper(), path, {**headers, **auth}, body)
                assert status == 401, (operation, status)
                assert response_headers["WWW-Authenticate"] == "Bearer"
                assert json.loads(raw) == {"code": "UNAUTHENTICATED", "message": "UNAUTHENTICATED"}
            for actor, actor_role in ACTORS.items():
                response = http_request(stand.port, method.upper(), path, {
                    **headers, "Authorization": f"Bearer {stand.tokens[actor]}",
                }, body)
                if actor_role != role:
                    assert response[0] == 403, (operation, actor, response[0])
                    assert json.loads(response[2]) == {"code": "FORBIDDEN", "message": "FORBIDDEN"}
                else:
                    success = next(int(code) for code in op["responses"] if code.startswith("2"))
                    expected = 404 if missing in path else success
                    assert response[0] == expected, (operation, response[0])
        assert stand.request("GET", "/applications", 401, headers={
            "Authorization": "Bearer synthetic-service-credential",
        })["code"] == "UNAUTHENTICATED"


def test_customer_resource_isolation(setup_stand):
    stand = setup_stand
    with stand.serve():
        other = stand.draft("other")
        app, offer = prepare_offer(stand)
        attempt_path, attempt = start_attempt(stand, app, offer)
        verify(stand, attempt_path, attempt)
        process(stand, "VERIFY")
        contract = stand.request("GET", attempt_path, 200)["contract_id"]
        terms = f"/applications/{app}/terms/{offer['terms_id']}"
        random_id = str(uuid4())
        cases = [
            ("GET", f"/applications/{app}", app, None),
            ("PATCH", f"/applications/{app}", app, {"requested_term_months": 18}),
            ("POST", f"/applications/{app}/submission", app, None),
            ("POST", f"/applications/{app}/cancellation", app, None),
            ("GET", f"/applications/{app}/offer", app, None),
            ("GET", terms + "/document", app, None),
            ("PUT", terms + "/pdn-notice-acknowledgement", app, {"notice_version": offer["pdn_notice"]["version"]}),
            ("POST", f"/applications/{app}/signing-attempts", app, None),
            ("GET", attempt_path, app, None),
            ("POST", attempt_path + "/sms-resends", app, None),
            ("POST", attempt_path + "/verification", app, {"code": stand_code(attempt["attempt_id"])}),
            ("GET", f"/contracts/{contract}", contract, None),
            ("POST", f"/contracts/{contract}/withdrawal", contract, None),
        ]
        for method, path, resource, body in cases:
            key = uuid4()
            absent = stand.request(method, path.replace(resource, random_id), 404, "other", body, key)
            assert stand.request(method, path, 404, "other", body, key) == absent
        assert stand.request("GET", attempt_path.replace(app, other), 404, "other")["code"] == "NOT_FOUND"
        assert stand.request("GET", terms.replace(app, other) + "/document", 404, "other")["code"] == "NOT_FOUND"
        assert [entry["application_id"] for entry in stand.request("GET", "/applications", 200, "other")] == [other]
        assert_reserve(stand, "HELD")


def test_replay_requires_current_access_before_fingerprint(setup_stand):
    stand = setup_stand
    with stand.serve():
        create_key, update_key = uuid4(), uuid4()
        body = {"requested_term_months": 12}
        draft = stand.request("POST", "/applications", 201, body=body, key=create_key)
        path = f"/applications/{draft['application_id']}"
        stand.request("PATCH", path, 200, body=body, key=update_key)
        stand.sql("UPDATE stand_session SET customer_id=%s WHERE principal='mobile'", (OTHER_CUSTOMER,))
        for variant in (body, {"requested_term_months": 24}):
            assert stand.request("POST", "/applications", 403, body=variant, key=create_key)["code"] == "FORBIDDEN"
            assert stand.request("PATCH", path, 404, body=variant, key=update_key)["code"] == "NOT_FOUND"
        stand.sql("UPDATE stand_session SET customer_id=%s WHERE principal='mobile'", (CUSTOMER,))
        stand.sql("UPDATE stand_session SET enabled=false WHERE principal='mobile'")
        assert stand.request("PATCH", path, 403, body=body, key=update_key)["code"] == "FORBIDDEN"
        stand.sql("DELETE FROM stand_session WHERE principal='mobile'")
        assert stand.request("PATCH", path, 401, body=body, key=update_key)["code"] == "UNAUTHENTICATED"
        assert stand.sql("SELECT count(*) AS n FROM accepted_request") == [{"n": 2}]


def test_contact_context_and_replay(setup_stand):
    stand = setup_stand
    with stand.serve():
        contract = signed_contract(stand, "200000.01")
        path = f"/contact-center/contracts/{contract}"
        status = stand.request("GET", f"/contact-center/customers/{CUSTOMER}/status", 200, "contact")
        assert set(status["applications"][0]) == {"application_id", "status"}
        assert set(status["contracts"][0]) == {
            "contract_id", "status", "cooling_ends_at", "posted_at", "withdrawal_available_after_identification",
        }
        assert stand.request("GET", f"/branches/customers/{CUSTOMER}/presence", 200, "branch") == {
            "has_active_application": False, "has_active_contract": True,
        }
        token = stand.request("POST", path + "/identification", 200, "contact")["identification_token"]
        confirmation = {"X-Identification-Token": token}
        key = uuid4()

        def withdraw(expected):
            return stand.request("POST", path + "/withdrawal", expected, "contact", key=key, headers=confirmation)

        for field, value in (("principal", "different-operator"), ("session_ref", uuid4()),
                             ("identified_customer_id", OTHER_CUSTOMER)):
            original, = stand.sql(f"SELECT {field} FROM stand_session WHERE role='CONTACT_CENTER'")
            stand.sql(f"UPDATE stand_session SET {field}=%s WHERE role='CONTACT_CENTER'", (value,))
            assert withdraw(409)["code"] == "IDENTIFICATION_REQUIRED"
            stand.sql(f"UPDATE stand_session SET {field}=%s WHERE role='CONTACT_CENTER'", (original[field],))
        assert_reserve(stand, "HELD")
        receipt = withdraw(200)
        stand.clock.at += timedelta(minutes=5)
        assert withdraw(200) == receipt
        original_session, = stand.sql("SELECT session_ref FROM stand_session WHERE principal='contact'")
        stand.sql("UPDATE stand_session SET session_ref=%s WHERE principal='contact'", (uuid4(),))
        assert withdraw(409)["code"] == "IDEMPOTENCY_KEY_REUSED"
        stand.sql("UPDATE stand_session SET session_ref=%s WHERE principal='contact'", (original_session["session_ref"],))
        stand.sql("UPDATE stand_session SET identified_customer_id=%s WHERE principal='contact'", (OTHER_CUSTOMER,))
        assert withdraw(409)["code"] == "IDENTIFICATION_REQUIRED"
        stand.sql("UPDATE stand_session SET enabled=false WHERE principal='contact'")
        assert withdraw(403)["code"] == "FORBIDDEN"
        assert_reserve(stand, "RELEASED")


def test_abs_only_accepts_service_token(setup_stand, monkeypatch):
    stand = setup_stand
    token = "synthetic-abs-credential"
    monkeypatch.setenv("LOANFLOW_ABS_TOKEN", token)
    instruction = str(uuid4())
    path = f"/instructions/{instruction}"
    body = json.dumps({"account_ref": "synthetic-abs-account", "amount_minor": 4195632, "currency": "RUB"})
    with http_server(create_abs(stand.dsn, stand.clock)) as port:
        for method, suffix, payload in (("GET", "", None), ("PUT", "", body), ("PUT", "/closure", None)):
            headers = {"Content-Type": "application/json"}
            status, _, raw = http_request(port, method, path + suffix, headers, payload)
            assert (status, json.loads(raw)) == (401, {"instruction_id": instruction, "code": "UNAUTHENTICATED"})
            for credential in (*stand.tokens.values(), "synthetic-unknown"):
                auth = {**headers, "Authorization": f"Bearer {credential}"}
                status, _, raw = http_request(port, method, path + suffix, auth, payload)
                assert (status, json.loads(raw)) == (403, {"instruction_id": instruction, "code": "FORBIDDEN"})
        assert not stand.sql("SELECT 1 FROM training_abs.call")
        auth = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        for method, suffix, payload, state in (("GET", "", None, "NOT_FOUND"), ("PUT", "", body, "PENDING"),
                                               ("PUT", "/closure", None, "CLOSED")):
            status, _, raw = http_request(port, method, path + suffix, auth, payload)
            assert (status, json.loads(raw)) == (200, {"instruction_id": instruction, "state": state})


def test_abs_does_not_follow_redirects(monkeypatch):
    monkeypatch.setenv("LOANFLOW_ABS_TOKEN", "synthetic-no-redirect-token")
    received = []
    target = FastAPI()

    @target.get("/disposition")
    def disposition():
        received.append(True)
        return {}

    origin = FastAPI()
    with http_server(target) as target_port:
        @origin.get("/instructions/{instruction_id}")
        def redirect():
            return RedirectResponse(f"http://127.0.0.1:{target_port}/disposition", status_code=302)

        with http_server(origin) as origin_port:
            result = AbsClient(f"http://127.0.0.1:{origin_port}").call("READ", uuid4(), 2)

    assert result is None and received == []


def test_contact_confirmation_belongs_to_original_contract(setup_stand):
    stand = setup_stand
    with stand.serve():
        original = signed_contract(stand, "200000.01")
        path = f"/contact-center/contracts/{original}"
        token = stand.request("POST", path + "/identification", 200, "contact")["identification_token"]
        stand.request("POST", f"/contracts/{original}/withdrawal", 200)
        current = signed_contract(stand, "200000.01")
        status = stand.request("POST", f"/contact-center/contracts/{current}/withdrawal", 409, "contact",
                               key=uuid4(), headers={"X-Identification-Token": token})
        assert status["code"] == "IDENTIFICATION_REQUIRED"
        assert stand.request("GET", f"/contracts/{current}", 200)["status"] == "COOLING_OFF"


def test_revoked_underwriter_cannot_replay_decision(setup_stand):
    stand = setup_stand
    with stand.serve():
        app = submit(stand, term=37)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE")
        path = f"/underwriting/applications/{app}/decision"
        key = uuid4()
        body = {"command": "REJECT", "rejection_reason_code": "SYNTHETIC_DECISION_REASON"}
        receipt = stand.request("POST", path, 200, "underwriter", body, key)
        assert stand.request("POST", path, 200, "underwriter", body, key) == receipt
        stand.sql("UPDATE stand_session SET role='UNDERWRITING_SUPERVISOR' WHERE principal='underwriter'")
        assert stand.request("POST", path, 403, "underwriter", body, key)["code"] == "FORBIDDEN"
        assert stand.sql("SELECT count(*) AS n FROM final_decision") == [{"n": 1}]


def test_application_and_worker_diagnostics_hide_protected_values(setup_stand, monkeypatch, caplog):
    stand = setup_stand
    markers = [
        "СИНТЕТИЧЕСКОЕ_ФИО", "synthetic-birth-date", "synthetic-birth-place", "synthetic-passport",
        "synthetic-inn", "synthetic-snils", "synthetic-phone", "synthetic-email", "synthetic-address",
        "synthetic-account", "synthetic-card", "synthetic-balance", "synthetic-operation", "synthetic-posted-amount",
        "synthetic-credit-report", "synthetic-income", "synthetic-pdn", "synthetic-loan-terms",
    ]

    def fail(*_args, **_kwargs):
        raise RuntimeError(json.dumps(markers))

    with stand.serve():
        invalid_json = (b"[" * 2000) + json.dumps(markers).encode() + (b"]" * 2000)
        headers = {
            "Authorization": f"Bearer {stand.tokens['mobile']}",
            "Idempotency-Key": str(uuid4()),
            "Content-Type": "application/json",
        }
        status, _, raw = http_request(stand.port, "POST", "/applications", headers, invalid_json)
        assert (status, json.loads(raw)) == (400, {"code": "INVALID_REQUEST", "message": "INVALID_REQUEST"})

    with monkeypatch.context() as patch:
        patch.setattr(applications, "list_own_applications", fail)
        with stand.serve():
            error = stand.request("GET", "/applications", 503)
    assert error == {"code": "SERVICE_UNAVAILABLE", "message": "SERVICE_UNAVAILABLE"}

    with stand.serve():
        app, offer = prepare_offer(stand, "49999.00")
        attempt_path, attempt = start_attempt(stand, app, offer)
        code = stand_code(attempt["attempt_id"])
        key = uuid4()
        accepted = verify(stand, attempt_path, attempt, key)
        markers.extend([*stand.tokens.values(), code, "4999900", json.dumps({"code": code})])
        assert verify(stand, attempt_path, attempt, key) == accepted
        for row in stand.sql("SELECT request::text FROM accepted_request AS request"):
            assert all(value not in row["request"] for value in markers)
        for row in stand.sql("SELECT payload::text FROM service_job WHERE kind='VERIFY'"):
            assert all(value not in row["payload"] for value in markers)
        verification = worker(stand, checkpoint=fail)
        assert not verification.run_once("VERIFY")
        stand.clock.at += timedelta(seconds=1)
        process(stand, "VERIFY")
        with abs_server(stand, monkeypatch) as client:
            markers.append("synthetic-abs-test-token")
            sender = worker(stand, abs_client=client, checkpoint=lambda name, _: fail() if name == "after_send" else None)
            assert sender.run_once("SEND")
            instruction = instruction_id(stand)
            path = f"/reconciliation/instructions/{instruction}"
            receipt = register(stand, path, key)
            assert register(stand, path, key) == receipt
            stand.sql("UPDATE stand_session SET enabled=false WHERE principal='reconciliation'")
            assert stand.request("POST", path + "/reads", 403, "reconciliation", key=key)["code"] == "FORBIDDEN"

    assert "operation_failed operation=listOwnApplications" in caplog.text
    assert "job_processing_failed" in caplog.text and "kind=VERIFY" in caplog.text
    assert f"abs_send_failed instruction_id={instruction}" in caplog.text
    assert all(record.exc_info is None and record.stack_info is None for record in caplog.records
               if record.name.startswith("loanflow"))
    assert all(value not in caplog.text and value not in json.dumps(error, ensure_ascii=False) for value in markers)


def test_abs_and_check_http_errors_hide_input(setup_stand, monkeypatch, caplog):
    stand = setup_stand
    account = "synthetic-diagnostic-account"
    report = "synthetic-diagnostic-credit-report"
    amount = 7465912
    token = "synthetic-diagnostic-service-token"
    consent = "synthetic-diagnostic-consent"
    markers = [account, report, str(amount), token, consent]
    monkeypatch.setenv("LOANFLOW_ABS_TOKEN", token)

    def fail(*_):
        raise RuntimeError(" ".join(markers))

    instruction = str(uuid4())
    abs_request = json.dumps({"account_ref": account, "amount_minor": amount, "currency": "RUB"})
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    with http_server(create_abs(stand.dsn, stand.clock, fail)) as port:
        status, _, raw = http_request(port, "PUT", f"/instructions/{instruction}", headers, abs_request)
        assert status == 503
        assert json.loads(raw) == {"instruction_id": instruction, "code": "SERVICE_UNAVAILABLE"}
        assert not stand.sql("SELECT 1 FROM training_abs.call")

    checks_port = int(os.environ["LOANFLOW_CHECKS_URL"].rsplit(":", 1)[1])
    rest = json.dumps({"customer_id": CUSTOMER, "consent_ref": consent})
    soap = passport_message("CheckPassportRequest", {"customer_id": CUSTOMER})
    with monkeypatch.context() as patch:
        patch.setattr(store, "connect", fail)
        for path, headers, body in (
            ("/credit-reports", {"Content-Type": "application/json"}, rest),
            ("/fraud-checks", {"Content-Type": "application/json"}, json.dumps({"customer_id": CUSTOMER})),
            ("/passport-check", {"Content-Type": SOAP_TYPE, "SOAPAction": SOAP_ACTION}, soap),
        ):
            status, _, raw = http_request(checks_port, "POST", path, headers, body)
            assert (status, raw) == (503, b"")

    # Сбой построения ответа также должен остаться в диагностике без тела и трассировки.
    with monkeypatch.context() as patch:
        patch.setattr(store, "as_json", fail)
        status, _, raw = http_request(checks_port, "POST", "/credit-reports",
                                      {"Content-Type": "application/json"}, rest)
        assert (status, raw) == (503, b"")
    assert "operation_failed operation=sendInstruction" in caplog.text
    assert "operation_failed operation=getTrainingCreditReport" in caplog.text
    assert "operation_failed operation=checkTrainingFraud" in caplog.text
    with monkeypatch.context() as patch:
        patch.setattr(check_stubs, "passport_message", fail)
        status, _, raw = http_request(checks_port, "POST", "/passport-check",
                                      {"Content-Type": SOAP_TYPE, "SOAPAction": SOAP_ACTION}, soap)
        assert (status, raw) == (503, b"")
    assert "passport_check_failed" in caplog.text
    assert not any(record.exc_info or record.stack_info for record in caplog.records if record.name.startswith("loanflow"))
    assert all(value not in caplog.text for value in markers)


def test_postgres_server_logs_exclude_failed_row_and_error_text():
    protected = ("synthetic-sql-account", "synthetic-sql-income")
    with temporary_postgres() as (params, _):
        with psycopg.connect(**params, autocommit=True) as db:
            db.execute("CREATE TABLE private_fixture (account text, income text CHECK (income = ''))")
            with pytest.raises(psycopg.errors.CheckViolation):
                db.execute("INSERT INTO private_fixture VALUES (%s, %s)", protected)
            with pytest.raises(psycopg.errors.InvalidTextRepresentation):
                db.execute("SELECT %s::int", (protected[1],))
        diagnostic = (Path(params["host"]) / "postgres.log").read_text()
        assert all(value not in diagnostic for value in protected)


def test_seed_cli_keeps_fixture_values_out_of_diagnostics(tmp_path, monkeypatch, capsys):
    token_file = tmp_path / "tokens.json"
    protected = "synthetic-seed-protected-value"

    def fail(*_):
        raise ValueError(protected)

    monkeypatch.setattr(seed, "seed", fail)
    monkeypatch.setattr("sys.argv", ["seed", "seed", "--tokens", str(token_file)])
    with pytest.raises(SystemExit) as stopped:
        seed.main()
    assert "ValueError" in str(stopped.value)
    assert protected not in str(stopped.value) + capsys.readouterr().err
    assert not token_file.exists()
