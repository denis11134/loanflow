import hmac
import os
from time import sleep
from uuid import uuid4

from fastapi.responses import JSONResponse

from . import store
from .http import bearer, routes
from .store import ApiError, one

KEYS = ("account_ref", "amount_minor", "currency")


def lock(db, instruction_id):
    db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 325))", (str(instruction_id),))
    return one(db, "SELECT * FROM training_abs.instruction WHERE instruction_id=%s", (instruction_id,))


def post_pending(db, instruction_id, clock=store.now):
    row = lock(db, instruction_id)
    if row and row["state"] == "PENDING":
        return one(db, """UPDATE training_abs.instruction SET state='POSTED', posting_ref=%s, posted_at=%s
                          WHERE instruction_id=%s RETURNING *""", (str(uuid4()), clock(), instruction_id))
    return row


def payload(instruction_id, row):
    result = dict(instruction_id=instruction_id, state=row["state"] if row else "NOT_FOUND")
    if row and row["state"] == "POSTED":
        result.update({key: row[key] for key in (*KEYS, "posting_ref", "posted_at")})
    return result


def create_abs(dsn=None, clock=store.now, checkpoint=lambda *_: None):
    operations = {"sendInstruction": "SEND", "readInstruction": "READ", "closeInstruction": "CLOSE"}

    def invoke(op, request, _method, _path, args, _key, _identification, body, clock):
        instruction_id = args["instruction_id"]

        def error(status, code):
            return status, dict(instruction_id=instruction_id, code=code)

        try:
            token = bearer(request)
        except ApiError:
            return error(401, "UNAUTHENTICATED")
        if not hmac.compare_digest(token.encode(), os.environ["LOANFLOW_ABS_TOKEN"].encode()):
            return error(403, "FORBIDDEN")
        kind = operations[op["operationId"]]
        with store.connect(dsn) as db:
            db.execute("INSERT INTO training_abs.call (instruction_id,operation,called_at) VALUES (%s,%s,%s)",
                       (instruction_id, kind, clock()))
            row = lock(db, instruction_id)
            control = one(db, "SELECT behavior FROM training_abs.control WHERE instruction_id=%s AND operation=%s",
                          (instruction_id, kind))
            behavior = control["behavior"] if control else None
            if behavior == "UNAVAILABLE":
                return error(503, "SERVICE_UNAVAILABLE")
            if kind == "READ":
                if behavior == "READ_CONFLICT" and (not row or row["state"] == "PENDING"):
                    return error(409, "READ_CONFLICT")
            elif kind == "CLOSE":
                if not row:
                    row = one(db, """INSERT INTO training_abs.instruction (instruction_id,state)
                                     VALUES (%s,'CLOSED') RETURNING *""", (instruction_id,))
                elif row["state"] == "PENDING":
                    row = one(db, """UPDATE training_abs.instruction SET state='CLOSED'
                                     WHERE instruction_id=%s RETURNING *""", (instruction_id,))
            else:
                if row and row["account_ref"] is not None and any(body[key] != row[key] for key in KEYS):
                    return error(409, "REQUISITES_CONFLICT")
                if not row:
                    row = one(db, """INSERT INTO training_abs.instruction
                                     (instruction_id,state,account_ref,amount_minor,currency)
                                     VALUES (%s,'PENDING',%s,%s,%s) RETURNING *""",
                              (instruction_id, *(body[key] for key in KEYS)))
                if behavior == "AUTO_POST":
                    row = post_pending(db, instruction_id, clock)
            checkpoint("before_abs_commit", instruction_id, kind)
        checkpoint("after_abs_commit", instruction_id, kind)
        if behavior == "DROP_RESPONSE":
            sleep(2.2)
            return error(503, "SERVICE_UNAVAILABLE")
        return 200, payload(instruction_id, row)

    def invalid(op, args):
        body = {"code": "INVALID_INSTRUCTION"}
        if "instruction_id" in args:
            body["instruction_id"] = str(args["instruction_id"])
        return JSONResponse(body, status_code=422 if op["operationId"] == "sendInstruction" else 400)

    return routes("abs", invoke, invalid, clock)


app = create_abs()
