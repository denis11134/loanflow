import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[2]


def connect(dsn=None):
    return psycopg.connect(dsn or os.environ["LOANFLOW_DSN"], row_factory=dict_row)


def migrate(dsn=None):
    files = [ROOT / "sql/001_initial_schema.sql", *sorted((ROOT / "services/migrations").glob("*.sql"))]
    with connect(dsn) as db:
        db.execute("SELECT pg_advisory_xact_lock(301, 1)")
        db.execute("CREATE TABLE IF NOT EXISTS service_migration (name text PRIMARY KEY)")
        applied = {r["name"] for r in db.execute("SELECT name FROM service_migration")}
        for path in files:
            if path.name not in applied:
                sql = path.read_text().strip().removeprefix("BEGIN;").removesuffix("COMMIT;")
                db.execute(sql)
                db.execute("INSERT INTO service_migration VALUES (%s)", (path.name,))


def now():
    return datetime.now(timezone.utc)


def digest(*parts):
    key = bytes.fromhex(os.environ["LOANFLOW_SECRET"])
    if len(key) < 32:
        raise ValueError("LOANFLOW_SECRET must have at least 32 bytes")
    data = json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hmac.digest(key, data, hashlib.sha256)


def json_value(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(type(value).__name__)


def as_json(value):
    return json.loads(json.dumps(value, default=json_value))


def enqueue(db, kind, resource_id, run_at, sequence=0, payload=None):
    db.execute(
        """INSERT INTO service_job (kind, resource_id, sequence, run_at, payload)
           VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (kind, resource_id, sequence, run_at, Jsonb(payload or {})),
    )


class ApiError(Exception):
    def __init__(self, status, code, **fields):
        self.status = status
        self.body = {"code": code, "message": code, **fields}


def one(db, sql, params=()):
    return db.execute(sql, params).fetchone()


def require(row):
    if row is None:
        raise ApiError(404, "NOT_FOUND")
    return row


def lock_customer(db, customer_id):
    return require(one(db, "SELECT * FROM customer WHERE customer_id=%s FOR UPDATE", (customer_id,)))


def lock_application(db, application_id):
    entry = require(one(db, "SELECT customer_id FROM application WHERE application_id=%s", (application_id,)))
    lock_customer(db, entry["customer_id"])
    return require(one(db, "SELECT * FROM application WHERE application_id=%s FOR UPDATE", (application_id,)))


def reserve_transition(db, application_id, state, at):
    reservation = one(db, """SELECT r.offer_id, r.quota_id FROM mpl_reservation r
                             JOIN offer o USING (offer_id) WHERE o.application_id=%s""", (application_id,))
    if reservation is None:
        return False
    db.execute("SELECT quota_id FROM mpl_quota WHERE quota_id=%s FOR UPDATE", (reservation["quota_id"],))
    return one(db, """UPDATE mpl_reservation SET state=%s, state_changed_at=%s
                      WHERE offer_id=%s AND state='HELD' RETURNING offer_id""",
               (state, at, reservation["offer_id"])) is not None
