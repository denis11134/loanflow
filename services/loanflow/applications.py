from decimal import Decimal
from uuid import uuid4

from .calculations import MAX_AMOUNT_RUB, MAX_TERM_MONTHS, MIN_AMOUNT_RUB, MIN_TERM_MONTHS, RULES_VERSION
from .store import ApiError, enqueue, lock_application, one, require, reserve_transition

ACTIVE = ("SCORING", "UNDERWRITING", "APPROVED", "SIGNING_IN_PROGRESS")
BLOCKING_CONTRACT = ("COOLING_OFF", "DISBURSEMENT_PROCESSING", "DISBURSED")
CANCELLABLE = ("DRAFT", "SCORING", "UNDERWRITING", "APPROVED")
NOTICE_VERSION = "v1"


def rubles(value):
    return None if value is None else Decimal(int(value)).scaleb(-2)


def minor(value):
    return None if value is None else str(int(value * 100))


def presence(db, customer_id, excluded_application=None):
    return one(db, """
        SELECT EXISTS (SELECT FROM application WHERE customer_id=%(customer)s AND status=ANY(%(active)s)
                       AND application_id IS DISTINCT FROM %(exclude)s::uuid) AS has_active_application,
               (EXISTS (SELECT FROM contract c JOIN application a USING (application_id)
                        WHERE a.customer_id=%(customer)s AND c.status=ANY(%(blocking)s))
                OR EXISTS (SELECT FROM external_contract_ref WHERE customer_id=%(customer)s AND is_active))
               AS has_active_contract
        """, dict(customer=customer_id, active=list(ACTIVE), blocking=list(BLOCKING_CONTRACT),
                  exclude=excluded_application))


def own_application(ctx, application_id, lock=False):
    row = require(one(ctx.db, "SELECT * FROM application WHERE application_id=%s AND customer_id=%s",
                      (application_id, ctx.actor["customer_id"])))
    return lock_application(ctx.db, application_id) if lock else row


def offer_row(db, application_id):
    return one(db, """
        SELECT o.*, t.terms_id, t.document_sha256, t.first_provided_at, t.offer_expires_at,
               t.warning_first_shown_at, t.warning_text_version, r.state AS reserve_state,
               CASE WHEN s.rules_version IS NULL THEN e.pdn_percent > 50
                    ELSE (e.monthly_existing_payments_rub + e.requested_monthly_payment_rub) * 100
                         > e.monthly_payroll_income_rub * 50
               END AS pdn_notice_required
        FROM offer o LEFT JOIN individual_terms t USING (offer_id)
        JOIN mpl_reservation r USING (offer_id) LEFT JOIN evaluation e ON e.application_id=o.application_id
        JOIN submission_snapshot s ON s.application_id=o.application_id
        WHERE o.application_id=%s
        """, (application_id,))


def notice_required(offer):
    return bool(offer["pdn_notice_required"])


def view(ctx, row):
    app_id = row["application_id"]
    status = row["status"]
    details = one(ctx.db, """
        SELECT s.requested_amount_rub, s.requested_term_months, c.contract_id, d.rejection_reason_code
        FROM application a LEFT JOIN submission_snapshot s USING (application_id)
        LEFT JOIN contract c USING (application_id) LEFT JOIN final_decision d USING (application_id)
        WHERE a.application_id=%s
        """, (app_id,))
    offer = offer_row(ctx.db, app_id)
    attempt = None
    expires = None
    availability = "NONE"
    actions = []
    if status == "DRAFT":
        actions.extend(("EDIT_DRAFT", "SUBMIT"))
    if status in CANCELLABLE:
        actions.append("CANCEL_APPLICATION")
    if offer:
        expires = offer["offer_expires_at"]
        if offer["terms_id"]:
            availability = "EXPIRED" if ctx.clock() >= expires else "AVAILABLE"
            actions.append("OPEN_TERMS")
            attempt = one(ctx.db, "SELECT attempt_id FROM signing_attempt WHERE terms_id=%s AND closed_at IS NULL",
                          (offer["terms_id"],))
            if (status == "APPROVED" and availability == "AVAILABLE" and not attempt
                    and offer["reserve_state"] == "HELD" and
                    (not notice_required(offer) or offer["warning_text_version"] == NOTICE_VERSION)):
                actions.append("START_SIGNING")
        elif status == "APPROVED":
            availability = "PREPARING"
    reason = None
    if status == "BANK_REJECTED":
        code = details["rejection_reason_code"]
        reason = code if code in ("SELF_BAN", "CREDIT_REPORT_CONSENT_REQUIRED") else "REFUSED"
    return {
        "application_id": app_id, "status": status, "created_at": row["created_at"],
        "submitted_at": row["submitted_at"],
        "requested_amount_minor": minor(details["requested_amount_rub"] if row["submitted_at"]
                                        else row["draft_amount_rub"]),
        "requested_term_months": details["requested_term_months"] if row["submitted_at"]
        else row["draft_term_months"],
        "offer_availability": availability, "offer_expires_at": expires,
        "active_attempt_id": attempt["attempt_id"] if attempt else None,
        "contract_id": details["contract_id"] if status == "CONTRACT_SIGNED" else None,
        "display_reason": reason, "available_actions": actions,
    }


def create_draft(ctx):
    row = one(ctx.db, """
        INSERT INTO application (application_id,customer_id,status,created_at,draft_amount_rub,draft_term_months)
        VALUES (%s,%s,'DRAFT',%s,%s,%s) RETURNING *
        """, (uuid4(), ctx.actor["customer_id"], ctx.clock(), rubles(ctx.body.get("requested_amount_minor")),
              ctx.body.get("requested_term_months")))
    return 201, view(ctx, row)


def list_own_applications(ctx):
    rows = ctx.db.execute("""SELECT * FROM application WHERE customer_id=%s
                             ORDER BY created_at DESC,application_id""", (ctx.actor["customer_id"],))
    return 200, [view(ctx, row) for row in rows]


def get_own_application(ctx, application_id):
    return 200, view(ctx, own_application(ctx, application_id))


def update_draft(ctx, application_id):
    app = own_application(ctx, application_id, lock=True)
    if app["status"] != "DRAFT":
        raise ApiError(409, "DRAFT_REQUIRED")
    amount = rubles(ctx.body["requested_amount_minor"]) if "requested_amount_minor" in ctx.body else app["draft_amount_rub"]
    row = one(ctx.db, """UPDATE application SET draft_amount_rub=%s,draft_term_months=%s
                         WHERE application_id=%s RETURNING *""",
              (amount, ctx.body.get("requested_term_months", app["draft_term_months"]), application_id))
    return 200, view(ctx, row)


def submit_application(ctx, application_id):
    db = ctx.db
    app = own_application(ctx, application_id, lock=True)
    submitted_at = app["submitted_at"]
    if submitted_at is None:
        if app["status"] != "DRAFT":
            raise ApiError(409, "APPLICATION_STATE_CONFLICT")
        amount, term = app["draft_amount_rub"], app["draft_term_months"]
        if amount is None or term is None:
            raise ApiError(422, "REQUIRED_FIELDS_MISSING")
        if not (MIN_AMOUNT_RUB <= amount <= MAX_AMOUNT_RUB and MIN_TERM_MONTHS <= term <= MAX_TERM_MONTHS):
            raise ApiError(422, "APPLICATION_PARAMETERS_OUT_OF_RANGE")
        profile = one(db, """SELECT is_payroll, version_ref FROM customer
                             LEFT JOIN stand_profile USING (customer_id) WHERE customer_id=%s""",
                      (app["customer_id"],))
        if not profile["is_payroll"]:
            raise ApiError(422, "CUSTOMER_NOT_ELIGIBLE")
        if any(presence(db, app["customer_id"], application_id).values()):
            raise ApiError(409, "BLOCKING_APPLICATION_OR_CONTRACT")
        if profile["version_ref"] is None:
            raise ApiError(503, "SERVICE_UNAVAILABLE")
        submitted_at = ctx.clock()
        db.execute("UPDATE application SET status='SCORING',submitted_at=%s WHERE application_id=%s",
                   (submitted_at, application_id))
        db.execute("""INSERT INTO submission_snapshot (application_id,requested_amount_rub,
                      requested_term_months,is_payroll,profile_version_ref,rules_version)
                      VALUES (%s,%s,%s,%s,%s,%s)""",
                   (application_id, amount, term, True, profile["version_ref"], RULES_VERSION))
        enqueue(db, "AML", application_id, submitted_at)
    return 202, {"application_id": application_id, "submitted_at": submitted_at, "accepted_status": "SCORING"}


def cancel_application(ctx, application_id):
    app = own_application(ctx, application_id, lock=True)
    if app["status"] != "CLIENT_CANCELED":
        if app["status"] not in CANCELLABLE:
            raise ApiError(409, "APPLICATION_CANCELLATION_UNAVAILABLE")
        at = ctx.clock()
        if app["status"] == "APPROVED":
            reserve_transition(ctx.db, application_id, "RELEASED", at)
        ctx.db.execute("""UPDATE application SET status='CLIENT_CANCELED',
                          underwriting_exited_at=CASE WHEN status='UNDERWRITING' THEN %s
                                                     ELSE underwriting_exited_at END WHERE application_id=%s""",
                       (at, application_id))
    return 200, {"application_id": application_id, "status": "CLIENT_CANCELED"}
