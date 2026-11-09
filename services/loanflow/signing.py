from datetime import timedelta
from hmac import compare_digest
from uuid import uuid4

from .applications import NOTICE_VERSION, notice_required, offer_row, own_application
from .assessment import adapter_outcome, audit, scenario_for
from .contracts import create_signed_contract
from .offers import cooling_hours
from .store import ApiError, digest, enqueue, lock_application, one, require, reserve_transition

ATTEMPT_SQL = """SELECT s.*,t.application_id,t.offer_expires_at,c.contract_id FROM signing_attempt s
                 JOIN individual_terms t USING (terms_id) LEFT JOIN contract c USING (attempt_id)"""


def attempt_row(db, attempt_id):
    return require(one(db, ATTEMPT_SQL + " WHERE s.attempt_id=%s", (attempt_id,)))


def own_attempt(ctx, application_id, attempt_id, lock=False):
    app = own_application(ctx, application_id, lock)
    attempt = attempt_row(ctx.db, attempt_id)
    if attempt["application_id"] != application_id:
        raise ApiError(404, "NOT_FOUND")
    return app, attempt


def attempt_deadline(attempt):
    return min(attempt["offer_expires_at"], attempt["expires_at"])


def start_signing_attempt(ctx, application_id):
    app = own_application(ctx, application_id, lock=True)
    offer = offer_row(ctx.db, application_id)
    at = ctx.clock()
    if app["status"] == "OFFER_EXPIRED" or (offer and offer["terms_id"] and at >= offer["offer_expires_at"]):
        raise ApiError(409, "OFFER_EXPIRED")
    if app["status"] == "SIGNING_IN_PROGRESS":
        raise ApiError(409, "SIGNING_ALREADY_IN_PROGRESS")
    if app["status"] != "APPROVED" or not offer or offer["reserve_state"] != "HELD":
        raise ApiError(409, "APPLICATION_STATE_CONFLICT")
    if not offer["terms_id"]:
        raise ApiError(409, "TERMS_NOT_PROVIDED")
    if notice_required(offer) and offer["warning_text_version"] != NOTICE_VERSION:
        raise ApiError(409, "PDN_NOTICE_NOT_ACKNOWLEDGED")
    attempt_id, expires = uuid4(), at + timedelta(minutes=5)
    db = ctx.db
    db.execute("""INSERT INTO signing_attempt (attempt_id,terms_id,opened_at,expires_at,verification_state)
                  VALUES (%s,%s,%s,%s,'AWAITING_CODE')""", (attempt_id, offer["terms_id"], at, expires))
    db.execute("UPDATE application SET status='SIGNING_IN_PROGRESS' WHERE application_id=%s", (application_id,))
    enqueue(db, "SMS", attempt_id, at)
    enqueue(db, "ATTEMPT_EXPIRY", attempt_id, expires)
    return 201, dict(application_id=application_id, attempt_id=attempt_id, expires_at=expires,
                     offer_expires_at=offer["offer_expires_at"])


def get_signing_attempt(ctx, application_id, attempt_id):
    _, attempt = own_attempt(ctx, application_id, attempt_id)
    state = {"AWAITING_CODE": "OPEN", "VERIFYING": "VERIFYING", "CLOSED": "CLOSED"}[attempt["verification_state"]]
    remaining = 2 - attempt["sms_resend_count"]
    can_verify = state == "OPEN" and ctx.clock() < attempt_deadline(attempt)
    result = dict(attempt_id=attempt_id, state=state, expires_at=attempt["expires_at"],
                  offer_expires_at=attempt["offer_expires_at"], resends_remaining=remaining,
                  can_resend=can_verify and remaining > 0, can_verify=can_verify)
    if attempt["contract_id"]:
        result.update(state="SIGNED", contract_id=attempt["contract_id"])
    elif state == "CLOSED":
        result["close_reason"] = attempt["close_reason"]
    return 200, result


def check_open(db, attempt, at):
    if at >= attempt["offer_expires_at"] or attempt["close_reason"] == "OFFER_EXPIRED":
        raise ApiError(409, "OFFER_EXPIRED")
    current = one(db, "SELECT attempt_id FROM signing_attempt WHERE terms_id=%s AND closed_at IS NULL",
                  (attempt["terms_id"],))
    if current and current["attempt_id"] != attempt["attempt_id"]:
        raise ApiError(409, "SIGNING_ATTEMPT_NOT_CURRENT")
    if attempt["close_reason"] == "ATTEMPT_EXPIRED" or (not attempt["closed_at"] and at >= attempt["expires_at"]):
        raise ApiError(409, "SIGNING_ATTEMPT_EXPIRED")
    if attempt["closed_at"]:
        raise ApiError(409, "SIGNING_ATTEMPT_CLOSED")
    if attempt["verification_state"] == "VERIFYING":
        raise ApiError(409, "VERIFICATION_IN_PROGRESS")


def resend_signing_sms(ctx, application_id, attempt_id):
    _, attempt = own_attempt(ctx, application_id, attempt_id, lock=True)
    at = ctx.clock()
    check_open(ctx.db, attempt, at)
    number = attempt["sms_resend_count"] + 1
    if number > 2:
        raise ApiError(409, "SMS_RESEND_LIMIT")
    ctx.db.execute("UPDATE signing_attempt SET sms_resend_count=%s WHERE attempt_id=%s", (number, attempt_id))
    enqueue(ctx.db, "SMS", attempt_id, at, number)
    return 202, dict(attempt_id=attempt_id, resend_number=number, accepted_at=at)


def verify_signing_code(ctx, application_id, attempt_id):
    _, attempt = own_attempt(ctx, application_id, attempt_id, lock=True)
    at = ctx.clock()
    check_open(ctx.db, attempt, at)
    ctx.db.execute("UPDATE signing_attempt SET verification_state='VERIFYING' WHERE attempt_id=%s", (attempt_id,))
    enqueue(ctx.db, "VERIFY", attempt_id, at, payload={
        "code_digest": digest("verification", attempt_id, ctx.body["code"]).hex(),
        "session_ref": str(ctx.actor["session_ref"]), "device_ref": str(ctx.actor["device_ref"]),
    })
    return 202, dict(attempt_id=attempt_id, accepted_at=at)


def stand_code(attempt_id):
    """Синтетический SMS-адаптер воспроизводит код только внутри доверенного процесса."""
    value = int.from_bytes(digest("stand-sms", str(attempt_id))[:8], "big") % 1_000_000
    return f"{value:06d}"


def close_attempt(db, attempt_id, reason, at):
    db.execute("""UPDATE signing_attempt SET verification_state='CLOSED',closed_at=%s,close_reason=%s
                  WHERE attempt_id=%s AND closed_at IS NULL""", (at, reason, attempt_id))


def expire_offer(db, app, offer, at):
    if app["status"] not in ("APPROVED", "SIGNING_IN_PROGRESS") or at < offer["offer_expires_at"]:
        return
    db.execute("""UPDATE signing_attempt SET verification_state='CLOSED',closed_at=%s,close_reason='OFFER_EXPIRED'
                  WHERE terms_id=%s AND closed_at IS NULL""", (at, offer["terms_id"]))
    reserve_transition(db, app["application_id"], "RELEASED", at)
    db.execute("UPDATE application SET status='OFFER_EXPIRED' WHERE application_id=%s", (app["application_id"],))


def offer_expiry_job(db, app_id, clock):
    app = lock_application(db, app_id)
    offer = offer_row(db, app_id)
    if offer and offer["terms_id"]:
        expire_offer(db, app, offer, clock())


def fail_attempt(db, app_id, attempt_id, reason, at):
    close_attempt(db, attempt_id, reason, at)
    db.execute("UPDATE application SET status='APPROVED' WHERE application_id=%s", (app_id,))


def run_attempt_job(db, job, clock, checkpoint):
    attempt_id, kind = job["resource_id"], job["kind"]
    before = attempt_row(db, attempt_id)
    if before["closed_at"]:
        return None
    app_id = before["application_id"]
    outcome = "OK"
    if kind in ("SMS", "VERIFY") and clock() < attempt_deadline(before):
        scenario = scenario_for(db, app_id) or {}
        failure_index = job["sequence"] if kind == "SMS" else job["failures"]
        outcome = adapter_outcome(scenario, kind, failure_index)
        audit(db, kind, attempt_id, clock(), job["sequence"])
        if kind == "VERIFY" and outcome == "OK":
            delivered = one(db, """SELECT verification_digest FROM stand_sms_delivery
                                   WHERE attempt_id=%s ORDER BY sequence LIMIT 1""", (attempt_id,))
            if delivered is None:
                outcome = "WAITING_FOR_SMS"
            elif not compare_digest(bytes(delivered["verification_digest"]).hex(), job["payload"]["code_digest"]):
                outcome = "INVALID_CODE"
        checkpoint("after_" + kind.lower(), job)
    app = lock_application(db, app_id)
    attempt = attempt_row(db, attempt_id)
    if app["status"] != "SIGNING_IN_PROGRESS" or attempt["closed_at"]:
        return None
    at = clock()
    offer = offer_row(db, app_id)
    if at >= offer["offer_expires_at"]:
        expire_offer(db, app, offer, at)
    elif at >= attempt["expires_at"]:
        fail_attempt(db, app_id, attempt_id, "ATTEMPT_EXPIRED", at)
    elif kind == "SMS":
        if outcome in ("DELIVERY_FAILED", "PERMANENT"):
            fail_attempt(db, app_id, attempt_id, "DELIVERY_FAILED", at)
        elif outcome == "OK":
            db.execute("INSERT INTO stand_sms_delivery VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                       (attempt_id, job["sequence"], digest("verification", attempt_id, stand_code(attempt_id)), at))
    elif kind == "VERIFY" and attempt["verification_state"] == "VERIFYING":
        if outcome in ("INVALID_CODE", "CODE_EXPIRED", "DELIVERY_FAILED"):
            fail_attempt(db, app_id, attempt_id, outcome, at)
        elif outcome == "OK":
            close_attempt(db, attempt_id, "SIGNED", at)
            create_signed_contract(db, app, attempt, offer, at, job["payload"]["session_ref"],
                                   job["payload"]["device_ref"], cooling_hours(offer["amount_rub"]))
            checkpoint("before_sign_commit", job)
        else:
            return min(attempt_deadline(attempt), at + timedelta(seconds=1))
    return None
