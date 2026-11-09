from datetime import timedelta
from decimal import Decimal, localcontext
from io import BytesIO
from functools import lru_cache
from uuid import uuid4

from psycopg.types.json import Jsonb
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

from .calendar import holidays, offer_deadline, underwriting_deadline
from .store import enqueue, lock_application, one, reserve_transition

STAGES = ("AML", "CREDIT_REPORT", "DE", "PUBLISH")
SLA_TICK = timedelta(microseconds=1)


def scenario_for(db, application_id):
    row = one(db, """SELECT p.scenario FROM stand_profile p JOIN application a USING (customer_id)
                     JOIN submission_snapshot s USING (application_id)
                     WHERE a.application_id=%s AND p.version_ref=s.profile_version_ref""", (application_id,))
    return row["scenario"] if row else None


def audit(db, operation, resource_id, at, sequence=0):
    db.execute("""INSERT INTO stand_adapter_call (operation,resource_id,sequence,called_at)
                  VALUES (%s,%s,%s,%s)""", (operation, resource_id, sequence, at))


def adapter_outcome(scenario, kind, failures):
    plan = scenario.get("faults", {}).get(kind, [])
    return plan[failures] if failures < len(plan) else "OK"


def finish_decision(db, app, clock, parameters=None, reason=None, operator=None, command=None):
    app_id = app["application_id"]
    quota = None
    if parameters:
        if Decimal(parameters["psk_annual_percent"]) > Decimal(parameters["psk_limit"]):
            reason = "PSK_LIMIT_EXCEEDED"
        else:
            quota = one(db, """SELECT * FROM mpl_quota WHERE period_key=%s AND bucket_code=%s FOR UPDATE""",
                        (parameters["quota_period"], parameters["quota_bucket"]))
            if quota is None:
                raise RuntimeError("Missing stand quota")
            allocated = one(db, """SELECT count(*) AS used FROM mpl_reservation
                                   WHERE quota_id=%s AND state IN ('HELD','CONSUMED')""", (quota["quota_id"],))
            if allocated["used"] >= quota["capacity"]:
                reason = "MPL_QUOTA_EXHAUSTED"
    at = clock()
    approved = parameters is not None and reason is None
    decision = "APPROVED" if approved else "REJECTED"
    status = "APPROVED" if approved else "BANK_REJECTED"
    db.execute("""INSERT INTO final_decision
                  (application_id,decision_source,outcome,decided_at,rejection_reason_code,
                   underwriter_id,underwriter_command) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
               (app_id, "MANUAL" if operator else "AUTOMATIC", decision, at, reason, operator, command))
    if approved:
        offer_id = uuid4()
        db.execute("""INSERT INTO offer (offer_id,application_id,amount_rub,term_months,annual_rate_percent,
                      monthly_payment_rub,psk_annual_percent,psk_rub) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                   (offer_id, app_id, Decimal(parameters["amount_rub"]), parameters["term_months"],
                    Decimal(parameters["annual_rate_percent"]), Decimal(parameters["monthly_payment_rub"]),
                    Decimal(parameters["psk_annual_percent"]), Decimal(parameters["psk_rub"])))
        db.execute("INSERT INTO mpl_reservation VALUES (%s,%s,'HELD',%s)", (offer_id, quota["quota_id"], at))
        enqueue(db, "PUBLISH", app_id, at)
    db.execute("""UPDATE application SET status=%s, underwriting_exited_at=
                  CASE WHEN status='UNDERWRITING' THEN %s ELSE underwriting_exited_at END
                  WHERE application_id=%s""", (status, at, app_id))
    receipt = dict(application_id=app_id, command=command, decision=decision, application_status=status, decided_at=at)
    if reason is not None:
        receipt["rejection_reason_code"] = reason
    return receipt


@lru_cache(maxsize=1)
def terms_font():
    name = "DejaVuSans"
    pdfmetrics.registerFont(TTFont(name, "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"))
    return name


def render_terms(offer):
    output = BytesIO()
    doc = canvas.Canvas(output, invariant=True)
    doc.setTitle("LoanFlow · Индивидуальные условия")
    doc.setFont(terms_font(), 12)
    lines = [
        "LoanFlow · Индивидуальные условия · Учебный стенд",
        f"Предложение: {offer['offer_id']}",
        f"Сумма: {offer['amount_rub']:.2f} ₽",
        f"Срок: {offer['term_months']} мес.",
        f"Годовая ставка: {offer['annual_rate_percent']:f}%",
        f"Ежемесячный платёж: {offer['monthly_payment_rub']:.2f} ₽",
        f"ПСК годовых: {offer['psk_annual_percent']:f}%",
        f"ПСК в рублях: {offer['psk_rub']:.2f} ₽",
    ]
    for index, line in enumerate(lines):
        doc.drawString(72, 760 - 24 * index, line)
    doc.save()
    return output.getvalue()


def evaluate_aml(db, app, scenario, at):
    app_id = app["application_id"]
    blocked = scenario.get("aml_block", False)
    consent = scenario.get("consent_ref") if not blocked else None
    db.execute("""INSERT INTO evaluation (application_id,evaluated_at,aml_result,consent_ref)
                  VALUES (%s,%s,%s,%s)""", (app_id, at, "BLOCK" if blocked else "PASS", consent))
    if blocked or not consent:
        finish_decision(db, app, lambda: at, reason="AML_BLOCK" if blocked else "CREDIT_REPORT_CONSENT_REQUIRED")
    else:
        enqueue(db, "CREDIT_REPORT", app_id, at)


def evaluate_credit(db, app, scenario, at):
    app_id = app["application_id"]
    blocked = scenario.get("self_ban", False)
    db.execute("UPDATE evaluation SET self_ban_applies=%s,credit_report_ref=%s WHERE application_id=%s",
               (blocked, scenario.get("credit_report_ref"), app_id))
    if blocked:
        finish_decision(db, app, lambda: at, reason="SELF_BAN")
        return
    income, existing, requested = (Decimal(scenario[key]) for key in
                                   ("payroll_income_rub", "existing_payments_rub", "requested_payment_rub"))
    with localcontext(prec=38):
        pdn = (existing + requested) * 100 / income
    db.execute("""UPDATE evaluation SET monthly_payroll_income_rub=%s,monthly_existing_payments_rub=%s,
                  requested_monthly_payment_rub=%s,pdn_percent=%s WHERE application_id=%s""",
               (income, existing, requested, pdn, app_id))
    enqueue(db, "DE", app_id, at)


def evaluate_de(db, app, scenario, clock):
    app_id = app["application_id"]
    result = scenario["de_result"]
    parameters = scenario.get("offer")
    db.execute("UPDATE evaluation SET de_result=%s,recommendation=%s WHERE application_id=%s",
               (result, Jsonb(parameters) if parameters else None, app_id))
    if result == "MANUAL_REVIEW":
        at = clock()
        deadline = underwriting_deadline(at, holidays(db))
        db.execute("""UPDATE application SET status='UNDERWRITING',underwriting_entered_at=%s,
                      underwriting_deadline_at=%s WHERE application_id=%s""", (at, deadline, app_id))
        enqueue(db, "SLA", app_id, deadline + SLA_TICK)
    elif result == "REJECTED":
        finish_decision(db, app, clock, reason="DE_REJECTED")
    elif result == "APPROVED" and parameters:
        finish_decision(db, app, clock, parameters)
    else:
        raise ValueError("Invalid stand decision")


def run(db, job, clock, checkpoint):

    app_id = job["resource_id"]
    kind = job["kind"]
    needed = "APPROVED" if kind == "PUBLISH" else "SCORING"
    state = one(db, "SELECT status FROM application WHERE application_id=%s", (app_id,))
    if state["status"] != needed:
        return None
    scenario = scenario_for(db, app_id)
    failure = adapter_outcome(scenario, kind, job["failures"]) if scenario is not None else "PERMANENT"
    audit(db, kind, app_id, clock(), job["failures"] + 1)
    offer = one(db, "SELECT * FROM offer WHERE application_id=%s", (app_id,)) if kind == "PUBLISH" else None
    pdf = render_terms(offer) if offer and failure == "OK" else None
    checkpoint("after_" + kind.lower(), job)
    app = lock_application(db, app_id)
    if app["status"] != needed:
        return None
    at = clock()
    if failure != "OK":
        failures = job["failures"] + 1
        if failure != "PERMANENT" and failures < 3:
            return at + timedelta(seconds=1 if failures == 1 else 3)
        reserve_transition(db, app_id, "RELEASED", at)
        db.execute("UPDATE application SET status='SYSTEM_ERROR' WHERE application_id=%s", (app_id,))
    elif kind == "AML":
        evaluate_aml(db, app, scenario, at)
    elif kind == "CREDIT_REPORT":
        evaluate_credit(db, app, scenario, at)
    elif kind == "DE":
        evaluate_de(db, app, scenario, clock)
    else:
        deadline = offer_deadline(at, holidays(db))
        db.execute("""INSERT INTO individual_terms (terms_id,offer_id,application_id,document_bytes,content_type,
                      first_provided_at,offer_expires_at) VALUES (%s,%s,%s,%s,'application/pdf',%s,%s)
                      ON CONFLICT (offer_id) DO NOTHING""", (uuid4(), offer["offer_id"], app_id, pdf, at, deadline))
        enqueue(db, "OFFER_EXPIRY", app_id, deadline)
    return None
