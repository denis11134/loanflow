"""Формирование задания и однократное применение рекомендации в ядре."""
from decimal import Decimal
from uuid import uuid4

from psycopg.types.json import Jsonb

from . import assessment, event_bus, store


def income_input(scenario):
    if "payroll_income_rub" not in scenario:
        return {"state": "INVALID_SOURCE"}
    value = scenario["payroll_income_rub"]
    if value is None:
        return {"state": "UNCONFIRMED"}
    try:
        amount = assessment.source_amount(value)
        return {"state": "AVAILABLE", "amount_minor": str(int(amount * 100))}
    except (ArithmeticError, ValueError, TypeError):
        return {"state": "INVALID_SOURCE"}


def request_assessment(db, application_id, at):
    inputs = assessment.assessment_inputs(db, application_id)
    scenario = inputs["scenario"]
    consent = scenario.get("consent_ref")
    if consent is not None and not isinstance(consent, str):
        consent_input = {"state": "INVALID_SOURCE"}
    elif not consent or not consent.strip():
        consent_input = {"state": "ABSENT"}
    else:
        consent_input = {"state": "GRANTED", "consent_ref": consent}
    try:
        quota = store.quota_reference(scenario)
        quota_input = dict(zip(("period", "bucket"), quota)) if quota and all(x.strip() for x in quota) else None
    except ValueError:
        quota_input = None
    payload = {
        "message_id": str(uuid4()), "application_id": str(application_id),
        "submitted_at": at.isoformat(),
        "inputs": {
            "customer_id": str(inputs["customer_id"]),
            "requested_amount_minor": str(int(inputs["requested_amount_rub"] * 100)),
            "requested_term_months": inputs["requested_term_months"],
            "rules_version": inputs["rules_version"],
            "profile_version_ref": store.one(db, """SELECT profile_version_ref FROM submission_snapshot
                                                    WHERE application_id=%s""", (application_id,))["profile_version_ref"],
            "aml_block": bool(scenario.get("aml_block", False)), "consent": consent_input,
            "payroll_income": income_input(scenario), "quota_ref": quota_input,
        },
    }
    db.execute("INSERT INTO assessment_dispatch VALUES (%s,%s,%s,NULL)",
               (application_id, payload["message_id"], at))
    event_bus.queue(db, event_bus.REQUEST_TOPIC, payload, at)


def apply_result(db, payload, clock):
    application_id = payload["application_id"]
    if store.one(db, "SELECT 1 FROM application WHERE application_id=%s", (application_id,)) is None:
        return "UNKNOWN_APPLICATION"
    app = store.lock_application(db, application_id)
    dispatch = store.one(db, "SELECT * FROM assessment_dispatch WHERE application_id=%s", (application_id,))
    if dispatch is None or dispatch["submitted_at"] != event_bus.timestamp(payload["submitted_at"]):
        return "SUBMISSION_MISMATCH"
    result = payload["result"]
    if dispatch["accepted_result"] is not None:
        return None if dispatch["accepted_result"] == result else "RESULT_CONFLICT"
    if app["status"] != "SCORING":
        return None
    # Отметка применения коммитится вместе с решением и резервом.
    db.execute("UPDATE assessment_dispatch SET accepted_result=%s WHERE application_id=%s",
               (Jsonb(result), application_id))
    at = clock()
    db.execute("INSERT INTO evaluation (application_id,evaluated_at) VALUES (%s,%s)", (application_id, at))
    outcome = result["outcome"]
    if outcome == "TECHNICAL_FAILURE":
        assessment.system_error(db, application_id, at)
    elif outcome == "BUSINESS_REJECTED":
        assessment.finish_decision(db, app, clock, reason=result["reason_code"])
    else:
        snapshot = store.one(db, "SELECT * FROM submission_snapshot WHERE application_id=%s", (application_id,))
        terms = result["terms"]
        parameters = {
            "amount_rub": str(snapshot["requested_amount_rub"]), "term_months": snapshot["requested_term_months"],
            "annual_rate_percent": terms["annual_rate_percent"],
            "monthly_payment_rub": str(Decimal(terms["monthly_payment_minor"]) / 100),
            "psk_annual_percent": terms["psk_annual_percent"], "psk_rub": terms["psk_rub"],
            "psk_limit_exceeded": result["psk_limit_exceeded"],
            "quota_period": result["quota_ref"]["period"], "quota_bucket": result["quota_ref"]["bucket"],
        }
        db.execute("""UPDATE evaluation SET recommendation=%s,pdn_notice_required=%s,de_result=%s
                      WHERE application_id=%s""",
                   (Jsonb(parameters), result["pdn_notice_required"],
                    "APPROVED" if outcome == "RECOMMEND_APPROVAL" else "MANUAL_REVIEW", application_id))
        if outcome == "MANUAL_REVIEW":
            assessment.enter_underwriting(db, application_id, at)
        else:
            assessment.finish_decision(db, app, clock, parameters)
    return None
