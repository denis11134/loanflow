from datetime import timedelta
from secrets import token_urlsafe
from uuid import uuid4

from .applications import minor, presence
from .store import ApiError, digest, enqueue, lock_customer, one, require, reserve_transition

CONTRACTS = """SELECT c.*,a.customer_id,e.verified_at,f.posted_at
               FROM contract c JOIN application a USING (application_id)
               JOIN signature_evidence e USING (attempt_id)
               LEFT JOIN disbursement_instruction i USING (contract_id)
               LEFT JOIN abs_final_result f USING (instruction_id)"""


def contract_row(db, contract_id, customer_id=None):
    return require(one(db, CONTRACTS + """ WHERE c.contract_id=%s
                     AND (%s::uuid IS NULL OR a.customer_id=%s)""", (contract_id, customer_id, customer_id)))


def lock_contract(db, contract_id, customer_id=None):
    row = contract_row(db, contract_id, customer_id)
    lock_customer(db, row["customer_id"])
    db.execute("SELECT contract_id FROM contract WHERE contract_id=%s FOR UPDATE", (contract_id,))
    return contract_row(db, contract_id, customer_id)


def start_disbursement(db, contract_id, account_ref, amount_rub):
    instruction = uuid4()
    db.execute("INSERT INTO disbursement_instruction VALUES (%s,%s,%s,%s,'RUB')",
               (instruction, contract_id, account_ref, int(minor(amount_rub))))
    db.execute("INSERT INTO disbursement_outbox (instruction_id,state) VALUES (%s,'WAITING')", (instruction,))


def create_signed_contract(db, app, attempt, offer, at, session_ref, device_ref, hours):
    app_id = app["application_id"]
    attempt_id = attempt["attempt_id"]
    db.execute("""INSERT INTO signature_evidence (attempt_id,terms_id,application_id,terms_sha256,
                  verification_result,channel,verified_at,session_ref,device_ref)
                  VALUES (%s,%s,%s,%s,'SUCCESS','SMS',%s,%s,%s)""",
               (attempt_id, offer["terms_id"], app_id, offer["document_sha256"], at, session_ref, device_ref))
    db.execute("UPDATE application SET status='CONTRACT_SIGNED' WHERE application_id=%s", (app_id,))
    contract_id = uuid4()
    account_ref = one(db, "SELECT account_ref FROM customer WHERE customer_id=%s", (app["customer_id"],))["account_ref"]
    status = "COOLING_OFF" if hours else "DISBURSEMENT_PROCESSING"
    cooling_ends_at = at + timedelta(hours=hours) if hours else None
    db.execute("""INSERT INTO contract (contract_id,application_id,attempt_id,status,account_ref,cooling_ends_at)
                  VALUES (%s,%s,%s,%s,%s,%s)""",
               (contract_id, app_id, attempt_id, status, account_ref, cooling_ends_at))
    if hours:
        enqueue(db, "COOLING", contract_id, cooling_ends_at)
    else:
        start_disbursement(db, contract_id, account_ref, offer["amount_rub"])


def cooling_elapsed(db, contract_id, clock):
    contract = lock_contract(db, contract_id)
    if contract["status"] != "COOLING_OFF":
        return None
    if clock() < contract["cooling_ends_at"]:
        return contract["cooling_ends_at"]
    db.execute("UPDATE contract SET status='DISBURSEMENT_PROCESSING' WHERE contract_id=%s", (contract_id,))
    offer = one(db, "SELECT amount_rub FROM offer WHERE application_id=%s", (contract["application_id"],))
    start_disbursement(db, contract_id, contract["account_ref"], offer["amount_rub"])
    return None


def get_own_contract(ctx, contract_id):
    row = contract_row(ctx.db, contract_id, ctx.actor["customer_id"])
    return 200, dict(contract_id=contract_id, application_id=row["application_id"], status=row["status"],
                     signed_at=row["verified_at"], cooling_ends_at=row["cooling_ends_at"], posted_at=row["posted_at"],
                     can_withdraw=row["status"] == "COOLING_OFF")


def withdraw(db, row, at):
    db.execute("UPDATE contract SET status='CANCELED_BY_CLIENT' WHERE contract_id=%s", (row["contract_id"],))
    if not reserve_transition(db, row["application_id"], "RELEASED", at):
        raise RuntimeError("Expected held reserve")


def unavailable(row):
    return ApiError(409, "WITHDRAWAL_UNAVAILABLE", contract_status=row["status"])


def withdraw_in_mobile(ctx, contract_id):
    row = lock_contract(ctx.db, contract_id, ctx.actor["customer_id"])
    if row["status"] == "COOLING_OFF":
        withdraw(ctx.db, row, ctx.clock())
    elif row["status"] != "CANCELED_BY_CLIENT":
        raise unavailable(row)
    return 200, dict(contract_id=contract_id, status="CANCELED_BY_CLIENT")


def identify_contact_center_customer(ctx, contract_id):
    row = lock_contract(ctx.db, contract_id)
    if row["status"] != "COOLING_OFF":
        raise unavailable(row)
    actor = ctx.actor
    if actor["identified_customer_id"] != row["customer_id"]:
        raise ApiError(409, "IDENTIFICATION_FAILED")
    token = token_urlsafe(32)
    expires = ctx.clock() + timedelta(minutes=5)
    ctx.db.execute("INSERT INTO contact_identification VALUES (%s,%s,%s,%s,%s,%s)",
                   (digest("identification", token), actor["session_ref"], actor["principal"],
                    row["customer_id"], contract_id, expires))
    return 200, dict(identification_token=token, expires_at=expires)


def withdraw_in_contact_center(ctx, contract_id):
    row = lock_contract(ctx.db, contract_id)
    if row["status"] != "COOLING_OFF":
        raise unavailable(row)
    actor = ctx.actor
    confirmation = one(ctx.db, """SELECT expires_at FROM contact_identification
                                   WHERE (token_digest,session_ref,principal,customer_id,contract_id)
                                   = (%s,%s,%s,%s,%s)""",
                       (digest("identification", ctx.identification), actor["session_ref"],
                        actor["principal"], row["customer_id"], contract_id))
    if not confirmation:
        raise ApiError(409, "IDENTIFICATION_REQUIRED")
    at = ctx.clock()
    if at >= confirmation["expires_at"]:
        raise ApiError(409, "IDENTIFICATION_EXPIRED")
    withdraw(ctx.db, row, at)
    return 200, dict(contract_id=contract_id, status="CANCELED_BY_CLIENT")


def get_contact_center_status(ctx, customer_id):
    require(one(ctx.db, "SELECT customer_id FROM customer WHERE customer_id=%s", (customer_id,)))
    applications = list(ctx.db.execute("""SELECT application_id,status FROM application WHERE customer_id=%s
                                          ORDER BY created_at DESC,application_id""", (customer_id,)))
    contracts = [dict(contract_id=r["contract_id"], status=r["status"], cooling_ends_at=r["cooling_ends_at"],
                      posted_at=r["posted_at"], withdrawal_available_after_identification=r["status"] == "COOLING_OFF")
                 for r in ctx.db.execute(CONTRACTS + " WHERE a.customer_id=%s ORDER BY e.verified_at DESC,c.contract_id",
                                         (customer_id,))]
    return 200, dict(customer_id=customer_id, applications=applications, contracts=contracts)


def check_branch_customer_presence(ctx, customer_id):
    require(one(ctx.db, "SELECT customer_id FROM customer WHERE customer_id=%s", (customer_id,)))
    return 200, presence(ctx.db, customer_id)
