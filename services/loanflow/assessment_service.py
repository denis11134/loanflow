"""Возобновляемый каскад в отдельной базе сервиса оценки."""
import json
import logging
import os
from datetime import timedelta
from decimal import Decimal
from http.client import HTTPException
from urllib.error import HTTPError
from urllib.request import Request, build_opener
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from . import calculations, event_bus, store
from .checks import ChecksClient
from .http import NoRedirect

log = logging.getLogger("loanflow.assessment")
RETRY_DELAYS = (1, 3)
STAGES = ("AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE")
APPLICATION_STATUSES = {
    "DRAFT", "SCORING", "UNDERWRITING", "APPROVED", "BANK_REJECTED", "CLIENT_CANCELED",
    "OFFER_EXPIRED", "SYSTEM_ERROR", "SIGNING_IN_PROGRESS", "CONTRACT_SIGNED",
}


def migrate(dsn):
    with store.connect(dsn) as db:
        db.execute("SELECT pg_advisory_xact_lock(533, 1)")
        event_bus.migrate(db)
        db.execute((store.ROOT / "services/loanflow/assessment_store.sql").read_text())


class CoreStateClient:
    def __init__(self, url=None):
        self.url = url or os.environ["LOANFLOW_CORE_URL"]
        self.opener = build_opener(NoRedirect())

    def active(self, task):
        request = Request(self.url + f"/internal/applications/{task['application_id']}/scoring-state",
                          headers={"Authorization": f"Bearer {os.environ['LOANFLOW_ASSESSMENT_TOKEN']}"})
        try:
            with self.opener.open(request, timeout=2) as response:
                payload = json.load(response)
            if (set(payload) != {"application_id", "submitted_at", "status"}
                    or payload["status"] not in APPLICATION_STATUSES
                    or UUID(payload["application_id"]) != task["application_id"]):
                return None
            return (event_bus.timestamp(payload["submitted_at"]) == task["submitted_at"]
                    and payload["status"] == "SCORING")
        except HTTPError as exc:
            with exc:
                if exc.code != 404:
                    return None
                try:
                    return False if json.load(exc) == {"code": "NOT_FOUND"} else None
                except (ValueError, OSError, HTTPException):
                    return None
        except (OSError, HTTPException, ValueError, TypeError, KeyError, RecursionError):
            return None


def accept_request(db, payload, clock):
    application_id = payload["application_id"]
    fingerprint = store.digest("assessment-request", payload["inputs"], payload["submitted_at"])
    db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 534))", (application_id,))
    existing = store.one(db, "SELECT * FROM assessment_task WHERE application_id=%s", (application_id,))
    if existing:
        return None if bytes(existing["request_fingerprint"]) == fingerprint else "REQUEST_CONFLICT"
    engine = assessment_engine()
    db.execute("""INSERT INTO assessment_task
                  (application_id,submitted_at,request_fingerprint,inputs,stage,run_at,engine)
                  VALUES (%s,%s,%s,%s,'AML',%s,%s)""",
               (application_id, event_bus.timestamp(payload["submitted_at"]), fingerprint,
                Jsonb(payload["inputs"]), clock(), engine))
    return None


def assessment_engine():
    engine = os.environ.get("LOANFLOW_ASSESSMENT_ENGINE", "local")
    if engine not in ("local", "camunda"):
        raise ValueError("Unknown assessment engine")
    return engine


def reject(reason):
    return {"outcome": "BUSINESS_REJECTED", "reason_code": reason}


def failed(kind="PERMANENT"):
    return {"outcome": "TECHNICAL_FAILURE", "failure_kind": kind}


def calculate(inputs, existing_minor, manual=False):
    principal = int(inputs["requested_amount_minor"])
    term = inputs["requested_term_months"]
    payment, payments, interest = calculations.annuity_schedule(principal, term, calculations.MONTHLY_RATE)
    pdn = calculations.pdn_percent(int(inputs["payroll_income"]["amount_minor"]), existing_minor, payment)
    scoring = calculations.scoring_result(calculations.scoring_score(pdn, term))
    if scoring == "REJECTED" and not manual:
        return None, scoring
    quota = inputs["quota_ref"]
    if quota is None:
        raise ValueError("Missing quota reference")
    terms = {
        "annual_rate_percent": str(calculations.ANNUAL_RATE_PERCENT),
        "monthly_payment_minor": str(payment),
        "psk_annual_percent": format(calculations.annual_psk_percent(principal, payments), ".12f"),
        "psk_rub": format(calculations.rubles_from_cents(interest), ".2f"),
    }
    recommendation = {
        "terms": terms, "quota_ref": quota, "pdn_notice_required": pdn > 50,
        "psk_limit_exceeded": calculations.present_value_difference(
            principal, payments, calculations.PSK_LIMIT_PERCENT) > 0,
    }
    return recommendation, scoring


class AssessmentService:
    def __init__(self, dsn, clock=store.now, state_client=None, checks_client=None, checkpoint=lambda *_: None):
        self.dsn = dsn
        self.clock = clock
        self.state = state_client or CoreStateClient()
        self.checks = checks_client or ChecksClient()
        self.checkpoint = checkpoint

    def run_once(self):
        with store.connect(self.dsn) as db:
            task = store.one(db, """SELECT * FROM assessment_task
                                    WHERE engine='local'
                                    AND stage IN ('AML','PASSPORT','CREDIT_REPORT','FRAUD','DE')
                                    AND run_at<=%s ORDER BY run_at,application_id
                                    FOR UPDATE SKIP LOCKED LIMIT 1""", (self.clock(),))
            if task is None:
                return False
            try:
                with db.transaction():
                    self.process_task(db, task)
            except Exception as exc:
                db.execute("UPDATE assessment_task SET run_at=%s WHERE application_id=%s",
                           (self.clock() + timedelta(seconds=1), task["application_id"]))
                log.error("assessment_step_failed category=%s", type(exc).__name__)
        return True

    def process_task(self, db, task):
        active = self.state.active(task)
        if active is None:
            db.execute("UPDATE assessment_task SET run_at=%s WHERE application_id=%s",
                       (self.clock() + timedelta(seconds=1), task["application_id"]))
            return
        if not active:
            db.execute("UPDATE assessment_task SET stage='CANCELED' WHERE application_id=%s",
                       (task["application_id"],))
            return
        self.checkpoint("before_assessment_step", task)
        result, next_stage, data, transport = self.step(db, task)
        self.checkpoint("after_assessment_step", task)
        if transport == "TEMPORARY" and task["failures"] < 2:
            db.execute("""UPDATE assessment_task SET failures=failures+1,run_at=%s
                          WHERE application_id=%s""",
                       (self.clock() + timedelta(seconds=RETRY_DELAYS[task["failures"]]), task["application_id"]))
        elif transport != "OK":
            self.complete(db, task, failed("RETRIES_EXHAUSTED" if transport == "TEMPORARY" else "PERMANENT"))
        elif result is not None:
            self.complete(db, task, result)
        else:
            db.execute("""UPDATE assessment_task SET stage=%s,data=%s,failures=0,run_at=%s
                          WHERE application_id=%s""", (next_stage, Jsonb(data), self.clock(), task["application_id"]))
        self.checkpoint("before_assessment_commit", task)

    def step(self, db, task):
        stage, inputs = task["stage"], task["inputs"]
        data = dict(task["data"])
        fault = store.one(db, """SELECT outcome FROM training_assessment_fault
                                WHERE application_id=%s AND stage=%s AND attempt=%s""",
                          (task["application_id"], stage, task["failures"] + 1))
        if fault:
            return None, stage, data, fault["outcome"]
        if inputs["rules_version"] != calculations.RULES_VERSION:
            return failed(), stage, data, "OK"
        if stage == "AML":
            return (reject("AML_BLOCK") if inputs["aml_block"] else None), "PASSPORT", data, "OK"
        if stage == "DE":
            if data["scoring_result"] == "REJECTED":
                return reject("SCORING_BELOW_THRESHOLD"), stage, data, "OK"
            result = dict(data["recommendation"], outcome=(
                "MANUAL_REVIEW" if data["scoring_result"] == "MANUAL_REVIEW" else "RECOMMEND_APPROVAL"))
            return result, stage, data, "OK"
        consent = inputs["consent"].get("consent_ref") if stage == "CREDIT_REPORT" else None
        outcome, response = self.checks.call(stage, UUID(inputs["customer_id"]), consent)
        if outcome != "OK":
            return None, stage, data, outcome
        if stage == "PASSPORT":
            if response["verdict"] == "INVALID":
                return reject("PASSPORT_INVALID"), stage, data, "OK"
            consent_state = inputs["consent"]["state"]
            if consent_state == "ABSENT":
                return reject("CREDIT_REPORT_CONSENT_REQUIRED"), stage, data, "OK"
            if consent_state == "INVALID_SOURCE":
                return failed(), stage, data, "OK"
            return None, "CREDIT_REPORT", data, "OK"
        if stage == "CREDIT_REPORT":
            if response["self_ban_applies"]:
                return reject("SELF_BAN"), stage, data, "OK"
            income = inputs["payroll_income"]
            try:
                existing = Decimal(response["existing_payments_rub"])
                if not existing.is_finite() or existing < 0 or existing * 100 != (existing * 100).to_integral_value():
                    raise ValueError
                if income["state"] == "INVALID_SOURCE":
                    raise ValueError
            except (KeyError, ValueError, ArithmeticError):
                return failed(), stage, data, "OK"
            if income["state"] == "UNCONFIRMED" or int(income["amount_minor"]) == 0:
                return reject("NO_CONFIRMED_INCOME"), stage, data, "OK"
            data["existing_minor"] = int(existing * 100)
            return None, "FRAUD", data, "OK"
        try:
            recommendation, scoring = calculate(inputs, data["existing_minor"], response["verdict"] == "SUSPECTED")
        except (ValueError, ArithmeticError, KeyError):
            return failed(), stage, data, "OK"
        if response["verdict"] == "SUSPECTED":
            return dict(recommendation, outcome="MANUAL_REVIEW"), stage, data, "OK"
        data.update(recommendation=recommendation, scoring_result=scoring)
        return None, "DE", data, "OK"

    def complete(self, db, task, result):
        payload = {"message_id": str(uuid4()), "application_id": str(task["application_id"]),
                   "submitted_at": task["submitted_at"].isoformat(), "result": result}
        event_bus.queue(db, event_bus.RESULT_TOPIC, payload, self.clock())
        db.execute("UPDATE assessment_task SET stage='COMPLETED',result=%s WHERE application_id=%s",
                   (Jsonb(result), task["application_id"]))
