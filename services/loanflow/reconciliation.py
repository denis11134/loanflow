from datetime import datetime, timedelta
from uuid import UUID

from .assessment import scenario_for
from .contracts import lock_contract
from .store import ApiError, enqueue, one, require, reserve_transition

READ_WINDOW = timedelta(seconds=10)
TERMINAL = ("POSTED", "CLOSED")
ALLOW_CLOSE = ("NOT_FOUND", "PENDING")

INSTRUCTIONS = """
    SELECT i.*, o.state AS outbox_state, c.application_id, c.status AS contract_status,
           f.result AS final_result
    FROM disbursement_instruction i JOIN contract c USING (contract_id)
    JOIN disbursement_outbox o USING (instruction_id) LEFT JOIN abs_final_result f USING (instruction_id)
"""


def instruction(db, instruction_id):
    return require(one(db, INSTRUCTIONS + " WHERE i.instruction_id=%s AND o.state<>'WAITING'", (instruction_id,)))


def lock_instruction(db, instruction_id):
    row = instruction(db, instruction_id)
    lock_contract(db, row["contract_id"])
    db.execute("SELECT instruction_id FROM disbursement_instruction WHERE instruction_id=%s FOR UPDATE", (instruction_id,))
    return instruction(db, instruction_id)


def last_read(db, instruction_id):
    return one(db, """SELECT instruction_id,read_seq,requested_at,result FROM abs_read
                      WHERE instruction_id=%s ORDER BY read_seq DESC LIMIT 1""", (instruction_id,))


def read_view(row):
    return {**row, "result": row["result"] or "WAITING"}


def close_accepted(db, instruction_id):
    return one(db, "SELECT 1 FROM abs_close_command WHERE instruction_id=%s", (instruction_id,)) is not None


def view(db, row):
    instruction_id = row["instruction_id"]
    latest = last_read(db, instruction_id)
    accepted = close_accepted(db, instruction_id)
    is_open = row["contract_status"] == "DISBURSEMENT_PROCESSING"
    result = dict(instruction_id=instruction_id, contract_id=row["contract_id"], contract_status=row["contract_status"],
                  close_command_accepted=accepted, can_close=bool(is_open and not accepted and latest
                                                                  and latest["result"] in ALLOW_CLOSE),
                  can_retry_close=is_open and accepted)
    if latest:
        result["last_read"] = read_view(latest)
    if row["final_result"]:
        result["final_result"] = row["final_result"]
    diagnostic = one(db, """SELECT operation,mismatch_kind,observed_at FROM abs_correlation_error
                            WHERE instruction_id=%s ORDER BY observed_at DESC,error_id DESC LIMIT 1""", (instruction_id,))
    if diagnostic:
        result["last_correlation_error"] = diagnostic
    return result


def list_reconciliation_queue(ctx):
    rows = ctx.db.execute(INSTRUCTIONS + " WHERE o.state='STARTED' ORDER BY o.started_at,i.instruction_id")
    return 200, [view(ctx.db, row) for row in rows]


def get_reconciliation(ctx, instruction_id):
    return 200, view(ctx.db, instruction(ctx.db, instruction_id))


def register_abs_read(ctx, instruction_id):
    db = ctx.db
    row = lock_instruction(db, instruction_id)
    if row["contract_status"] != "DISBURSEMENT_PROCESSING":
        raise ApiError(409, "RECONCILIATION_NOT_OPEN")
    latest = last_read(db, instruction_id)
    sequence = latest["read_seq"] + 1 if latest else 1
    at = ctx.clock()
    db.execute("INSERT INTO abs_read (instruction_id,read_seq,operator_id,requested_at) VALUES (%s,%s,%s,%s)",
               (instruction_id, sequence, ctx.actor["principal"], at))
    enqueue(db, "ABS_READ", instruction_id, at, sequence)
    return 202, dict(instruction_id=instruction_id, read_seq=sequence)


def get_abs_read_result(ctx, instruction_id, read_seq):
    return 200, read_view(require(one(ctx.db, """SELECT instruction_id,read_seq,requested_at,result
                                               FROM abs_read WHERE instruction_id=%s AND read_seq=%s""",
                                      (instruction_id, read_seq))))


def request_abs_closure(ctx, instruction_id):
    db = ctx.db
    row = lock_instruction(db, instruction_id)
    if row["final_result"]:
        return 200, {key: row[key] for key in ("instruction_id", "contract_id", "final_result", "contract_status")}
    if row["contract_status"] != "DISBURSEMENT_PROCESSING":
        raise ApiError(409, "RECONCILIATION_NOT_OPEN")
    at = ctx.clock()
    if not close_accepted(db, instruction_id):
        latest = last_read(db, instruction_id)
        if not latest or latest["result"] not in ALLOW_CLOSE:
            raise ApiError(409, "CLOSURE_NOT_AUTHORIZED_BY_READ")
        db.execute("INSERT INTO abs_close_command VALUES (%s,%s,%s,%s)",
                   (instruction_id, latest["read_seq"], ctx.actor["principal"], at))
    enqueue(db, "ABS_CLOSE", instruction_id, at)
    return 202, dict(instruction_id=instruction_id, close_command_accepted=True)


def response_uuid(body):
    try:
        return UUID(body.get("instruction_id", ""))
    except (TypeError, ValueError, AttributeError):
        return None


def mismatch_kind(row, body, response_id):
    if row["instruction_id"] != response_id:
        return "INSTRUCTION_ID"
    if body.get("state") == "POSTED":
        for field, kind in (("account_ref", "ACCOUNT"), ("amount_minor", "AMOUNT"), ("currency", "CURRENCY")):
            if body.get(field) != row[field]:
                return kind
    return None


def apply_final(db, row, body, at):
    if row["contract_status"] != "DISBURSEMENT_PROCESSING":
        return
    instruction_id = row["instruction_id"]
    posted = body["state"] == "POSTED"
    if not reserve_transition(db, row["application_id"], "CONSUMED" if posted else "RELEASED", at):
        raise RuntimeError("Expected held reserve")
    db.execute("""INSERT INTO abs_final_result (instruction_id,response_instruction_id,result,recorded_at,
                  posting_ref,account_ref,amount_minor,currency,posted_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
               (instruction_id, instruction_id, body["state"], at, body.get("posting_ref"), body.get("account_ref"),
                body.get("amount_minor"), body.get("currency"), body.get("posted_at")))
    db.execute("UPDATE contract SET status=%s WHERE contract_id=%s",
               ("DISBURSED" if posted else "DISBURSEMENT_FAILED", row["contract_id"]))
    db.execute("UPDATE disbursement_outbox SET state='COMPLETED',completed_at=%s WHERE instruction_id=%s",
               (at, instruction_id))
    if posted:
        observation = (scenario_for(db, row["application_id"]) or {}).get("observation")
        if observation:
            db.execute("INSERT INTO loan_observation VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                       (row["contract_id"], datetime.fromisoformat(observation["ends_at"]), observation.get("overdue")))


def handle_response(db, instruction_id, operation, response, clock, read_seq=None, window_end=None):
    row = lock_instruction(db, instruction_id)
    at = clock()
    if response is None:
        result, response_id = "TIMEOUT", None
    else:
        status, body, valid = response
        response_id = response_uuid(body)
        mismatch = mismatch_kind(row, body, response_id)
        result = "CORRELATION_ERROR" if mismatch else (
            body.get("state") if status == 200 and valid else
            "CONFLICT" if status == 409 and valid else "TIMEOUT")
        if result in ("TIMEOUT", "CORRELATION_ERROR"):
            response_id = None
        if mismatch and operation != "READ":
            db.execute("""INSERT INTO abs_correlation_error (instruction_id,operation,mismatch_kind,observed_at)
                          VALUES (%s,%s,%s,%s)""", (instruction_id, operation, mismatch, at))
        if result in TERMINAL:
            apply_final(db, row, body, at)
    if operation == "READ":
        if result not in TERMINAL and at >= window_end:
            result, response_id = "TIMEOUT", None
        db.execute("""UPDATE abs_read SET result=%s,response_instruction_id=%s
                      WHERE instruction_id=%s AND read_seq=%s
                      AND (result IS NULL OR (result='TIMEOUT' AND %s IN ('POSTED','CLOSED')))""",
                   (result, response_id, instruction_id, read_seq, result))
