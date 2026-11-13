from datetime import timedelta
from uuid import UUID, uuid4

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import pytest

from services.loanflow.check_stubs import create_stubs
from services.loanflow.checks import ChecksClient, SOAP_ACTION, SOAP_TYPE, passport_message
from services.loanflow.store import migrate
from test_service import http_server, process, setup_stand, submit, update_scenario, worker

pytestmark = pytest.mark.service


def test_suspected_fraud_saves_terms_before_manual_review(setup_stand):
    stand = setup_stand
    update_scenario(stand, {"fraud_verdict": "SUSPECTED", "payroll_income_rub": "40000.00"})
    with stand.serve():
        app = submit(stand)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD")
        assert stand.request("GET", f"/applications/{app}", 200)["status"] == "UNDERWRITING"
        [evaluation] = stand.sql("""SELECT recommendation, de_result, pdn_percent
                                   FROM evaluation WHERE application_id=%s""", (app,))
        assert evaluation["pdn_percent"] > 65
        assert evaluation["de_result"] is None
        terms = evaluation["recommendation"]
        assert terms["monthly_payment_rub"] == "4631.73"
        assert terms["psk_annual_percent"] == "20.000006146107"
        assert not worker(stand).run_once("DE")
        assert not stand.sql("SELECT 1 FROM offer")

        update_scenario(stand, {"payroll_income_rub": None, "quota_period": "unavailable"})
        decision = stand.request("POST", f"/underwriting/applications/{app}/decision", 200,
                                 "underwriter", {"command": "APPROVE"}, uuid4())
        assert decision["application_status"] == "APPROVED"
        process(stand, "PUBLISH")
        assert stand.request("GET", f"/applications/{app}/offer", 200)["monthly_payment_minor"] == "463173"


@pytest.mark.parametrize("recover", [True, False], ids=["recover", "exhaust"])
def test_http_503_retries_only_the_failed_check(setup_stand, recover):
    stand = setup_stand
    app = create_stubs(stand.dsn)
    calls = []

    @app.middleware("http")
    async def temporarily_unavailable(request: Request, call_next):
        calls.append(request.url.path)
        if request.url.path == "/credit-reports" and (not recover or calls.count("/credit-reports") <= 2):
            return Response(b"not JSON", status_code=503, media_type="text/plain")
        return await call_next(request)

    with http_server(app) as port, stand.serve():
        client = ChecksClient(f"http://127.0.0.1:{port}")
        application = submit(stand)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT", checks_client=client)
        for failures, delay in enumerate((1, 3), start=1):
            assert stand.sql("SELECT failures,run_at FROM service_job WHERE kind='CREDIT_REPORT'") == [
                {"failures": failures, "run_at": stand.clock() + timedelta(seconds=delay)}]
            restarted = worker(stand, checks_client=client)
            assert not restarted.run_once("CREDIT_REPORT")
            stand.clock.at += timedelta(seconds=delay)
            assert restarted.run_once("CREDIT_REPORT")
        if recover:
            process(stand, "FRAUD", "DE", "PUBLISH", checks_client=client)
        status = "APPROVED" if recover else "SYSTEM_ERROR"
        assert stand.request("GET", f"/applications/{application}", 200)["status"] == status
        expected = ["/passport-check", *(["/credit-reports"] * 3)]
        assert calls == (expected + ["/fraud-checks"] if recover else expected)


@pytest.mark.parametrize("failure", ["incomplete", "wrong_customer"])
def test_invalid_antifraud_result_is_permanent(setup_stand, failure):
    stand = setup_stand
    app = FastAPI()

    @app.post("/fraud-checks")
    def bad_fraud_answer(request: dict):
        if failure == "incomplete":
            return JSONResponse({"customer_id": request["customer_id"]})
        return JSONResponse({"customer_id": str(uuid4()), "verdict": "SUSPECTED"})

    with http_server(app) as port, stand.serve():
        application = submit(stand)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT")
        process(stand, "FRAUD", checks_client=ChecksClient(f"http://127.0.0.1:{port}"))
        assert stand.request("GET", f"/applications/{application}", 200)["status"] == "SYSTEM_ERROR"
        assert not stand.sql("SELECT 1 FROM service_job")
        assert not stand.sql("SELECT 1 FROM final_decision")


def test_passport_fault_for_unknown_customer(setup_stand):
    from urllib.error import HTTPError
    from urllib.request import Request as HttpRequest, urlopen

    stand = setup_stand
    with http_server(create_stubs(stand.dsn)) as port:
        url = f"http://127.0.0.1:{port}"
        unknown = uuid4()
        payload = passport_message("CheckPassportRequest", {"customer_id": str(unknown)})
        request = HttpRequest(url + "/passport-check", payload,
                              {"Content-Type": SOAP_TYPE, "SOAPAction": SOAP_ACTION})
        with pytest.raises(HTTPError) as error:
            urlopen(request, timeout=2)
        with error.value as response:
            assert response.code == 500
            assert b"<faultcode>soap:Client</faultcode>" in response.read()
        client = ChecksClient(url)
        assert client.call("PASSPORT", unknown) == ("PERMANENT", None)
        assert client.call("FRAUD", unknown) == ("PERMANENT", None)
        assert client.call("CREDIT_REPORT", UUID("10000000-0000-0000-0000-000000000001"), " ") == ("PERMANENT", None)


def test_migration_restarts_inflight_legacy_assessment(setup_stand):
    stand = setup_stand
    with stand.serve():
        app = submit(stand)
        process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD")
        stand.sql("UPDATE service_job SET failures=1 WHERE kind='DE'")
        stand.sql("DELETE FROM service_migration WHERE name='008_training_checks.sql'")
        migrate(stand.dsn)
        assert stand.sql("SELECT kind,failures,payload FROM service_job") == [
            {"kind": "PASSPORT", "failures": 0, "payload": {}}]
        assert stand.sql("SELECT aml_result,consent_ref,credit_report_ref,pdn_percent FROM evaluation") == [
            {"aml_result": "PASS", "consent_ref": None, "credit_report_ref": None, "pdn_percent": None}]
        stand.clock.at = stand.sql("SELECT run_at FROM service_job")[0]["run_at"]
        process(stand, "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE", "PUBLISH")
        assert stand.request("GET", f"/applications/{app}", 200)["status"] == "APPROVED"
