"""Camunda управляет каскадом; данные и идемпотентные эффекты принадлежат оценке."""
import base64
import json
import logging
import os
from urllib.error import HTTPError
from urllib.request import Request, build_opener
from uuid import UUID

from psycopg.types.json import Jsonb

from . import assessment_service, store
from .http import NoRedirect

log = logging.getLogger("loanflow.orchestration")
PROCESS_ID = "loanflow-assessment"
PREFIX = "loanflow-assessment-"
KINDS = ("register", *assessment_service.STAGES, "finalize")
STOP = {"route": "STOP"}


class InvalidJob(Exception):
    """Некорректное задание изолируется инцидентом без изменения решения."""


class CamundaError(Exception):
    def __init__(self, status):
        self.status = status
        super().__init__("Camunda request failed")


def engine_key(value):
    key = str(value)
    if (not key.isascii() or not key.isdecimal() or key != str(int(key))
            or not 0 < int(key) <= 2**63 - 1):
        raise ValueError("Invalid Camunda key")
    return key


class CamundaClient:
    def __init__(self, url=None):
        self.url = (url or os.environ["LOANFLOW_CAMUNDA_URL"]).rstrip("/")
        self.opener = build_opener(NoRedirect())
        self.definition_key = None

    def request(self, method, path, body=None, content_type="application/json"):
        data = body if isinstance(body, bytes) else (json.dumps(body).encode() if body is not None else None)
        headers = {"Content-Type": content_type, "Accept": "application/json"}
        username, password = os.environ.get("LOANFLOW_CAMUNDA_USERNAME"), os.environ.get("LOANFLOW_CAMUNDA_PASSWORD")
        if username is not None or password is not None:
            if username is None or password is None:
                raise ValueError("Both Camunda credentials are required")
            headers["Authorization"] = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
        request = Request(self.url + "/v2" + path, data=data, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=5) as response:
                data = response.read(2_000_001)
                if len(data) > 2_000_000:
                    raise ValueError("Camunda response too large")
                return json.loads(data) if data else None
        except HTTPError as exc:
            with exc:
                # Тело ошибки может содержать переменные, поэтому наружу выходит только статус.
                raise CamundaError(exc.code) from None

    def deploy(self):
        if self.definition_key is None:
            boundary = "loanflow-camunda-bpmn-boundary"
            model = (store.ROOT / "diagrams/loanflow-assessment.bpmn").read_bytes()
            body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"resources\"; "
                    "filename=\"loanflow-assessment.bpmn\"\r\nContent-Type: application/xml\r\n\r\n").encode()
            body += model + f"\r\n--{boundary}--\r\n".encode()
            result = self.request("POST", "/deployments", body, "multipart/form-data; boundary=" + boundary)
            definitions = [r["processDefinition"] for r in result["deployments"] if r.get("processDefinition")]
            definition, = [r for r in definitions if r["processDefinitionId"] == PROCESS_ID]
            self.definition_key = engine_key(definition["processDefinitionKey"])
        return self.definition_key

    def create(self, task):
        return self.request("POST", "/process-instances", {
            "processDefinitionKey": self.deploy(),
            "businessId": str(task["application_id"]) + ":" + task["submitted_at"].isoformat(),
            "variables": {"assessmentId": str(task["application_id"])},
        })

    def activate(self, kind):
        result = self.request("POST", "/jobs/activation", {
            "type": PREFIX + kind, "worker": "loanflow-assessment",
            "timeout": int(os.environ.get("LOANFLOW_CAMUNDA_JOB_TIMEOUT_MS", "30000")),
            "maxJobsToActivate": 4, "requestTimeout": -1,
            "fetchVariable": ["assessmentId"],
        })
        return result["jobs"]

    def complete(self, key, variables):
        return self.request("POST", f"/jobs/{key}/completion", {"variables": variables})

    def fail(self, key, retries, delay=1000):
        return self.request("POST", f"/jobs/{key}/failure", {
            "retries": retries, "retryBackOff": delay, "errorMessage": "LoanFlow worker unavailable",
        })


class CamundaAssessmentService(assessment_service.AssessmentService):
    def __init__(self, dsn, clock=store.now, state_client=None, checks_client=None,
                 checkpoint=lambda *_: None, client=None):
        super().__init__(dsn, clock, state_client, checks_client, checkpoint)
        self.client = client or CamundaClient()
        self.next_kind = 0

    def run_once(self):
        worked = False
        # Неудачный запуск не мешает подтверждать уже созданные задания.
        for operation in (self.launch_one, self.work_one):
            try:
                worked = operation() or worked
            except Exception as exc:
                log.error("camunda_operation_failed category=%s", type(exc).__name__)
        return worked

    def launch_one(self):
        with store.connect(self.dsn) as db:
            task = store.one(db, """SELECT * FROM assessment_task
                                    WHERE engine='camunda' AND NOT launch_ack
                                    AND stage NOT IN ('COMPLETED','CANCELED') AND launch_after<=now()
                                    ORDER BY launch_after,application_id FOR UPDATE SKIP LOCKED LIMIT 1""")
            if task is None:
                return False
            # Сохраняем отсрочку даже при отказе транспорта: соседние оценки получают очередь.
            db.execute("UPDATE assessment_task SET launch_after=now()+interval '1 second' WHERE application_id=%s",
                       (task["application_id"],))
            try:
                self.checkpoint("before_camunda_start", task)
                result = self.client.create(task)
                self.checkpoint("after_camunda_start", task)
                key = engine_key(result["processInstanceKey"])
                db.execute("""UPDATE assessment_task SET process_instance_key=%s,launch_ack=true
                              WHERE application_id=%s AND process_instance_key IS NULL""",
                           (key, task["application_id"]))
            except CamundaError as exc:
                if exc.status != 409:
                    log.error("camunda_start_failed category=HTTP_%s", exc.status)
            except Exception as exc:
                log.error("camunda_start_failed category=%s", type(exc).__name__)
        return True

    def work_one(self):
        kind = KINDS[self.next_kind]
        self.next_kind = (self.next_kind + 1) % len(KINDS)
        jobs = self.client.activate(kind)
        if not jobs:
            return False
        for job in jobs:
            try:
                response = self.handle_job(kind, job)
                self.checkpoint("after_camunda_step_commit", job)
                self.checkpoint("before_camunda_complete", job)
                self.client.complete(job["jobKey"], response)
                self.checkpoint("after_camunda_complete", job)
            except InvalidJob as exc:
                log.error("camunda_invalid_job category=%s", type(exc).__name__)
                try:
                    self.client.fail(job["jobKey"], 0)
                except Exception as error:
                    log.error("camunda_fail_unconfirmed category=%s", type(error).__name__)
            except Exception as exc:
                # Транспорт/БД не меняют число попыток бизнес-проверки.
                log.error("camunda_job_failed category=%s", type(exc).__name__)
                try:
                    # Пауза оставляет очередь открытой для соседних оценок.
                    self.client.fail(job["jobKey"], max(1, job.get("retries", 3)), delay=5000)
                except Exception:
                    pass  # После lease timeout движок снова выдаст задание.
        return True

    def handle_job(self, kind, job):
        try:
            app_id = UUID(job["variables"]["assessmentId"])
            key, instance = engine_key(job["jobKey"]), engine_key(job["processInstanceKey"])
            if (str(app_id) != job["variables"]["assessmentId"]
                    or job["type"] != PREFIX + kind or job["processDefinitionId"] != PROCESS_ID):
                raise ValueError
        except (ValueError, KeyError, TypeError, AttributeError):
            raise InvalidJob from None
        with store.connect(self.dsn) as db:
            task = store.one(db, "SELECT * FROM assessment_task WHERE application_id=%s FOR UPDATE", (app_id,))
            if task is None or task["engine"] != "camunda":
                return STOP
            receipt = store.one(db, "SELECT response FROM assessment_job_receipt WHERE job_key=%s", (key,))
            if receipt:
                return receipt["response"]
            if kind == "register":
                if task["stage"] in ("COMPLETED", "CANCELED") or task["process_instance_key"] not in (None, instance):
                    response = STOP
                else:
                    db.execute("""UPDATE assessment_task SET process_instance_key=%s,launch_ack=true
                                  WHERE application_id=%s""", (instance, app_id))
                    response = {"route": "NEXT"}
            elif task["process_instance_key"] != instance or task["stage"] in ("COMPLETED", "CANCELED"):
                response = STOP
            elif kind == "finalize" and task["stage"] != "RESULT_READY":
                raise InvalidJob("Result is not prepared")
            elif kind != "finalize" and kind != task["stage"]:
                raise InvalidJob("Unexpected assessment stage")
            else:
                response = self.perform(db, task, kind)
            db.execute("INSERT INTO assessment_job_receipt VALUES (%s,%s,%s)", (key, app_id, Jsonb(response)))
            self.checkpoint("before_assessment_commit", task)
        return response

    def perform(self, db, task, kind):
        active = self.state.active(task)
        if active is None:
            return {"route": "WAIT", "waitDuration": "PT1S"}
        if not active:
            db.execute("UPDATE assessment_task SET stage='CANCELED' WHERE application_id=%s", (task["application_id"],))
            return STOP
        if kind == "finalize":
            self.complete(db, task, task["prepared_result"])
            return {"route": "FINISH"}
        self.checkpoint("before_assessment_step", task)
        result, next_stage, data, transport = self.step(db, task)
        self.checkpoint("after_assessment_step", task)
        attempt = task["failures"] + 1
        if transport == "TEMPORARY" and task["failures"] < 2:
            db.execute("UPDATE assessment_task SET failures=failures+1 WHERE application_id=%s", (task["application_id"],))
            response = {"route": "WAIT", "waitDuration": f"PT{assessment_service.RETRY_DELAYS[task['failures']]}S"}
        else:
            if transport != "OK":
                result = assessment_service.failed("RETRIES_EXHAUSTED" if transport == "TEMPORARY" else "PERMANENT")
            if result is not None:
                db.execute("UPDATE assessment_task SET stage='RESULT_READY',prepared_result=%s WHERE application_id=%s",
                           (Jsonb(result), task["application_id"]))
                response = {"route": "FINISH"}
            else:
                db.execute("UPDATE assessment_task SET stage=%s,data=%s,failures=0 WHERE application_id=%s",
                           (next_stage, Jsonb(data), task["application_id"]))
                response = {"route": "NEXT"}
        db.execute("INSERT INTO assessment_step VALUES (%s,%s,%s,%s,%s)",
                   (task["application_id"], kind, attempt, Jsonb(response), self.clock()))
        return response
