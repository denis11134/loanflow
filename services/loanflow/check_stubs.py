import json
import logging
from decimal import Decimal, InvalidOperation
from uuid import UUID

from fastapi import Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from lxml import etree

from . import store
from .checks import SOAP, SOAP_ACTION, SOAP_TYPE, parse_passport, passport_message, passport_schema, soap_envelope
from .http import routes

log = logging.getLogger("loanflow.check_stubs")


def fixture_payments(value):
    try:
        amount = Decimal(str(value))
        if amount.is_finite() and amount >= 0 and amount == amount.quantize(Decimal("0.01")):
            return f"{abs(amount) if amount.is_zero() else amount:.2f}"
    except InvalidOperation:
        pass
    return value


def passport_fault(message):
    envelope, fault = soap_envelope(f"{{{SOAP}}}Fault")
    etree.SubElement(fault, "faultcode").text = "soap:Client"
    etree.SubElement(fault, "faultstring").text = message
    return Response(etree.tostring(envelope, encoding="utf-8"), status_code=500, media_type=SOAP_TYPE)


def create_stubs(dsn=None):
    passport_schema()

    def scenario_for(customer_id):
        with store.connect(dsn) as db:
            row = store.one(db, "SELECT scenario::text AS scenario FROM stand_profile WHERE customer_id=%s",
                            (UUID(customer_id),))
        return None if row is None else json.loads(row["scenario"], parse_float=str)

    def rest(_operation, _request, _method, path, _args, _key, _identification, body, _clock):
        customer_id = body["customer_id"]
        scenario = scenario_for(customer_id)
        if scenario is None:
            return 404, None
        if path == "/fraud-checks":
            return 200, {"customer_id": customer_id, "verdict": scenario.get("fraud_verdict", "CLEAR")}

        blocked = scenario.get("self_ban", False)
        result = {"customer_id": customer_id, "self_ban_applies": blocked}
        if blocked is False:
            if "credit_report_ref" in scenario:
                result["credit_report_ref"] = scenario["credit_report_ref"]
            if "existing_payments_rub" in scenario:
                result["existing_payments_rub"] = fixture_payments(scenario["existing_payments_rub"])
        return 200, result

    app = routes("checks", rest, lambda *_: Response(status_code=400))

    @app.post("/passport-check", include_in_schema=False)
    async def passport(request: Request):
        content_type = request.headers.get("Content-Type", "").partition(";")[0].strip().lower()
        if content_type != "text/xml" or request.headers.get("SOAPAction") != SOAP_ACTION:
            return passport_fault("Invalid request")
        try:
            fields = parse_passport(await request.body(), "CheckPassportRequest")
        except (ValueError, etree.LxmlError):
            return passport_fault("Invalid request")
        try:
            scenario = await run_in_threadpool(scenario_for, fields["customer_id"])
            if scenario is None:
                return passport_fault("Unknown customer")
            verdict = scenario.get("passport_verdict", "VALID")
            if isinstance(verdict, str):
                fields["verdict"] = verdict
            return Response(passport_message("CheckPassportResponse", fields), media_type=SOAP_TYPE)
        except Exception as exc:
            log.error("passport_check_failed category=%s sqlstate=%s", type(exc).__name__, getattr(exc, "sqlstate", None))
            return Response(status_code=503)

    return app


app = create_stubs()
