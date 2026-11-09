import json
import logging
import os
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from . import assessment, contracts, reconciliation, signing, store, underwriting
from .http import document, expand, validator
from .store import one

log = logging.getLogger("loanflow.worker")


def no_checkpoint(*_):
    pass


class AbsClient:
    def __init__(self, url=None):
        self.url = url
        doc = document("abs")
        paths = doc["paths"]
        operations = {
            "READ": paths["/instructions/{instruction_id}"]["get"],
            "SEND": paths["/instructions/{instruction_id}"]["put"],
            "CLOSE": paths["/instructions/{instruction_id}/closure"]["put"],
        }
        self.validators = {
            (kind, int(status)): validator(doc, expand(doc, response)["content"]["application/json"]["schema"])
            for kind, op in operations.items() for status, response in op["responses"].items()
        }

    def call(self, kind, instruction_id, timeout, body=None):
        base = self.url or os.environ["LOANFLOW_ABS_URL"]
        path = f"/instructions/{instruction_id}"
        if kind == "CLOSE":
            path += "/closure"
        request = Request(base + path, method="GET" if kind == "READ" else "PUT",
                          data=json.dumps(body).encode() if body is not None else None,
                          headers={"Authorization": f"Bearer {os.environ['LOANFLOW_ABS_TOKEN']}",
                                   "Content-Type": "application/json"})
        try:
            try:
                response = urlopen(request, timeout=timeout)
            except HTTPError as exc:
                response = exc
            with response:
                status, payload = response.status, json.load(response)
        except (OSError, URLError, ValueError):
            return None
        check = self.validators.get((kind, status))
        if check is None or not isinstance(payload, dict):
            return None
        return status, payload, check.is_valid(payload)


class Worker:
    def __init__(self, dsn=None, clock=store.now, abs_client=None, checkpoint=no_checkpoint):
        self.dsn = dsn
        self.clock = clock
        self.abs = abs_client or AbsClient()
        self.checkpoint = checkpoint

    def run_once(self, kind=None):
        with store.connect(self.dsn) as db:
            jobs = db.execute("""SELECT job_id FROM service_job WHERE run_at<=%s
                                 AND (%s::text IS NULL OR kind=%s) ORDER BY run_at,job_id""",
                              (self.clock(), kind, kind)).fetchall()
            for item in jobs:
                job_id = item["job_id"]
                if not one(db, "SELECT pg_try_advisory_lock(%s) AS locked", (job_id,))["locked"]:
                    continue
                # Блокировка задания живёт до закрытия соединения, включая сетевой вызов.
                job = one(db, "SELECT * FROM service_job WHERE job_id=%s AND run_at<=%s", (job_id, self.clock()))
                if job is None:
                    db.execute("SELECT pg_advisory_unlock(%s)", (job_id,))
                    continue
                self.checkpoint("before_job", job)
                next_run = self.run_job(db, job)
                if next_run is None:
                    db.execute("DELETE FROM service_job WHERE job_id=%s", (job_id,))
                else:
                    db.execute("UPDATE service_job SET run_at=%s,failures=failures+1 WHERE job_id=%s",
                               (next_run, job_id))
                return True
            if kind in (None, "SEND"):
                return self.send_one(db)
            return False

    def run_job(self, db, job):
        kind, resource = job["kind"], job["resource_id"]
        if kind in assessment.STAGES:
            return assessment.run(db, job, self.clock, self.checkpoint)
        if kind in ("VERIFY", "SMS", "ATTEMPT_EXPIRY"):
            return signing.run_attempt_job(db, job, self.clock, self.checkpoint)
        if kind == "OFFER_EXPIRY":
            return signing.offer_expiry_job(db, resource, self.clock)
        if kind == "COOLING":
            result = contracts.cooling_elapsed(db, resource, self.clock)
            self.checkpoint("before_cooling_commit", job)
            return result
        if kind == "SLA":
            return underwriting.escalate(db, resource, self.clock)
        if kind in ("ABS_READ", "ABS_CLOSE"):
            return self.reconcile(db, job)
        raise ValueError("Unknown job kind")

    def send_one(self, db):
        row = one(db, """WITH next AS (
                           SELECT instruction_id FROM disbursement_outbox WHERE state='WAITING'
                           ORDER BY instruction_id LIMIT 1 FOR UPDATE SKIP LOCKED
                        )
                        UPDATE disbursement_outbox o SET state='STARTED',started_at=%s
                        FROM next,disbursement_instruction i WHERE o.instruction_id=next.instruction_id
                        AND i.instruction_id=o.instruction_id AND o.state='WAITING' RETURNING i.*""", (self.clock(),))
        if row is None:
            return False
        self.checkpoint("before_started_commit", row)
        db.commit()
        self.checkpoint("after_started_commit", row)
        instruction_id = row["instruction_id"]
        response = self.abs.call("SEND", instruction_id, 2,
                                 {field: row[field] for field in ("account_ref", "amount_minor", "currency")})
        self.checkpoint("after_send", row)
        reconciliation.handle_response(db, instruction_id, "SEND", response, self.clock)
        self.checkpoint("before_abs_result_commit", row)
        return True

    def reconcile(self, db, job):
        instruction_id, sequence = job["resource_id"], job["sequence"]
        if job["kind"] == "ABS_CLOSE":
            row = reconciliation.instruction(db, instruction_id)
            db.commit()
            if not row["final_result"]:
                response = self.abs.call("CLOSE", instruction_id, 2)
                self.checkpoint("after_close", job)
                reconciliation.handle_response(db, instruction_id, "CLOSE", response, self.clock)
            return None

        read = one(db, """SELECT requested_at,result FROM abs_read WHERE instruction_id=%s AND read_seq=%s""",
                   (instruction_id, sequence))
        if read["result"] is not None:
            return None
        end = read["requested_at"] + reconciliation.READ_WINDOW
        db.commit()
        self.checkpoint("before_abs_read", job)
        remaining = (end - self.clock()).total_seconds()
        response = self.abs.call("READ", instruction_id, min(2, remaining)) if remaining > 0 else None
        self.checkpoint("after_read", job)
        reconciliation.handle_response(db, instruction_id, "READ", response, self.clock, sequence, end)
        return None


def main():
    logging.basicConfig(level=logging.INFO)
    worker = Worker()
    while True:
        try:
            if worker.run_once():
                continue
        except Exception:
            log.error("job_processing_failed")
        sleep(0.25)


if __name__ == "__main__":
    main()
