"""Внутренняя диагностика: только агрегаты, ограниченные метки и короткие проверки."""
import argparse
import asyncio
from collections import Counter
from contextlib import contextmanager
import hmac
import json
import logging
import math
import os
from threading import Lock, Thread
import time
from urllib.request import Request, build_opener

from fastapi import FastAPI, Request as HttpRequest
from fastapi.responses import JSONResponse, PlainTextResponse
import psycopg
from psycopg.rows import dict_row
import uvicorn

from . import store
from .http import NoRedirect

BUCKETS = (.01, .025, .05, .1, .25, .5, 1, 2, 5, 10, math.inf)
STAGES = ("AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE", "RESULT_READY", "COMPLETED", "CANCELED")
JOBS = (*STAGES[:5], "PUBLISH", "VERIFY", "SMS", "ATTEMPT_EXPIRY", "OFFER_EXPIRY", "COOLING", "SLA", "ABS_READ", "ABS_CLOSE")
ERRORS = {"event_operation_failed", "event_process_failed", "job_processing_failed", "abs_send_failed",
          "assessment_step_failed", "camunda_operation_failed", "camunda_start_failed", "camunda_invalid_job",
          "camunda_fail_unconfirmed", "camunda_job_failed"}


class Registry(logging.Handler):
    def __init__(self, operations=()):
        super().__init__(logging.ERROR)
        self.operations = frozenset(operations)
        self.guard = Lock()
        self.samples = {}
        self.errors = Counter({category: 0 for category in (*sorted(ERRORS), "other")})
        self.last_cycle = time.monotonic()

    def cycle(self):
        self.last_cycle = time.monotonic()

    def emit(self, record):
        # Ни текст исключения, ни аргументы записи не становятся метками.
        category = str(record.msg).split(" ", 1)[0]
        category = category if category in ERRORS else "other"
        with self.guard:
            self.errors[category] += 1

    def observe(self, operation, status, elapsed):
        operation = operation if operation in self.operations else "unmatched"
        status = f"{status // 100}xx" if 100 <= status < 600 else "other"
        with self.guard:
            sample = self.samples.setdefault((operation, status), [0, 0., [0] * len(BUCKETS)])
            sample[0] += 1
            sample[1] += elapsed
            for i, bound in enumerate(BUCKETS):
                sample[2][i] += elapsed <= bound

    def render(self):
        lines = ["# TYPE loanflow_http_request_duration_seconds histogram", "# TYPE loanflow_errors_total counter"]
        with self.guard:
            for (op, status), (count, total, buckets) in sorted(self.samples.items()):
                labels = f'operation="{op}",status_class="{status}"'
                for bound, value in zip(BUCKETS, buckets):
                    le = "+Inf" if math.isinf(bound) else str(bound)
                    lines.append(f'loanflow_http_request_duration_seconds_bucket{{{labels},le="{le}"}} {value}')
                lines += [f"loanflow_http_request_duration_seconds_count{{{labels}}} {count}",
                          f"loanflow_http_request_duration_seconds_sum{{{labels}}} {total}"]
            lines += [f'loanflow_errors_total{{category="{key}"}} {value}' for key, value in sorted(self.errors.items())]
        return "\n".join(lines) + "\n"


class HttpMetrics:
    def __init__(self, app, registry):
        self.app, self.registry = app, registry

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        start, status, recorded = time.monotonic(), 500, False

        async def outgoing(message):
            nonlocal status, recorded
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                recorded = True
                self.registry.observe(getattr(scope.get("route"), "name", "unmatched"), status, time.monotonic() - start)

        try:
            await self.app(scope, receive, outgoing)
        finally:
            if not recorded:
                self.registry.observe(getattr(scope.get("route"), "name", "unmatched"), status, time.monotonic() - start)


async def database_snapshot(dsn, role):
    # Общий бюджет включает соединение, блокировки, запросы и закрытие.
    async with asyncio.timeout(1.5):
        async with await psycopg.AsyncConnection.connect(
            dsn or os.environ["LOANFLOW_DSN"], row_factory=dict_row, connect_timeout=2,
            options="-c statement_timeout=800 -c lock_timeout=500 -c default_transaction_read_only=on",
        ) as db:
            async def rows(query, params=()):
                return await (await db.execute(query, params)).fetchall()

            # Отсутствующая миграция также означает неготовность, а не успешный SELECT 1.
            metrics = {}
            outbox, = await rows("""SELECT count(*) AS count, coalesce(greatest(0,
                extract(epoch FROM now()-min(created_at))),0)::float AS oldest_seconds
                FROM event_outbox WHERE sent_at IS NULL""")
            metrics["outbox"] = outbox
            metrics["rejections"] = await rows("SELECT category,count(*)::int AS count FROM event_rejection GROUP BY category")
            if role == "assessment":
                metrics["assessments"] = await rows("""SELECT stage,count(*)::int AS count FROM assessment_task GROUP BY stage""")
                stalled, = await rows("""SELECT count(*)::int AS count FROM assessment_task
                    WHERE stage NOT IN ('COMPLETED','CANCELED') AND submitted_at < now()-interval '180 seconds'
                    AND progress_at < now()-interval '60 seconds'
                    AND (wait_reason IS DISTINCT FROM 'RETRY' OR wait_until IS NULL OR wait_until<=now())""")
                metrics["stalled"] = stalled["count"]
                metrics["waiting"] = await rows("""SELECT wait_reason,count(*)::int AS count FROM assessment_task
                    WHERE stage NOT IN ('COMPLETED','CANCELED') AND wait_reason IS NOT NULL GROUP BY wait_reason""")
            else:
                # Требуем реальное наличие последней миграции, не только таблицы журнала.
                version = await rows("SELECT 1 FROM service_migration WHERE name='011_observability.sql'")
                if not version:
                    raise RuntimeError("Schema not ready")
                metrics["jobs"] = await rows("""SELECT kind,count(*)::int AS count,coalesce(greatest(0,
                    extract(epoch FROM now()-min(run_at))),0)::float AS oldest_seconds
                    FROM service_job WHERE run_at<=now() GROUP BY kind""")
                uncertain, = await rows("""SELECT count(*)::int AS count,coalesce(greatest(0,
                    extract(epoch FROM now()-min(o.started_at))),0)::float AS oldest_seconds
                    FROM disbursement_outbox o LEFT JOIN abs_final_result f USING(instruction_id)
                    WHERE o.state='STARTED' AND f.instruction_id IS NULL""")
                metrics["uncertain_instructions"] = uncertain
                stale, = await rows("""SELECT count(*)::int AS count FROM application
                    WHERE status='SCORING' AND submitted_at<now()-interval '180 seconds'""")
                metrics["scoring_overdue"] = stale["count"]
            return metrics


def kafka_snapshot(role, group=None):
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec
    from .event_bus import REQUEST_TOPIC, RESULT_TOPIC
    topic = RESULT_TOPIC if role == "core" else REQUEST_TOPIC
    group = group or os.environ.get("LOANFLOW_KAFKA_GROUP", f"loanflow-{role}-v1")
    deadline = time.monotonic() + 1.4

    def remaining():
        return max(.001, deadline - time.monotonic())

    admin = AdminClient({"bootstrap.servers": os.environ["LOANFLOW_KAFKA_BOOTSTRAP"],
                         "log_level": 0, "socket.timeout.ms": 1200})
    metadata = admin.list_topics(topic, timeout=remaining())
    info = metadata.topics[topic]
    if info.error:
        raise RuntimeError("Topic unavailable")
    partitions = [TopicPartition(topic, part) for part in info.partitions]
    committed = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group, partitions)], request_timeout=remaining())[group].result(remaining())
    ends = admin.list_offsets({p: OffsetSpec.latest() for p in partitions}, request_timeout=remaining())
    offsets = {p.partition: max(0, p.offset) for p in committed.topic_partitions}
    return {"lag": sum(max(0, future.result(remaining()).offset-offsets.get(p.partition, 0)) for p, future in ends.items())}


def http_probe(url, token=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with build_opener(NoRedirect()).open(Request(url, headers=headers), timeout=1.8) as response:
        payload = response.read(65537)
        if len(payload) > 65536:
            raise ValueError("Probe response too large")
        return json.loads(payload)


async def bounded(check):
    try:
        async with asyncio.timeout(2):
            return {"ready": True, "values": await check()}
    except Exception:
        return {"ready": False, "category": "unavailable"}


async def snapshot(dsn, role, registry, worker=False):
    checks = {"database": lambda: database_snapshot(dsn, role)}
    if os.environ.get("LOANFLOW_KAFKA_BOOTSTRAP"):
        checks["kafka"] = lambda: asyncio.to_thread(kafka_snapshot, role)
    if role == "assessment" and os.environ.get("LOANFLOW_CAMUNDA_URL"):
        url = os.environ.get("LOANFLOW_CAMUNDA_MANAGEMENT_URL", "http://127.0.0.1:9600")
        async def camunda():
            value = await asyncio.to_thread(http_probe, url + "/actuator/health/readiness")
            if value.get("status") != "UP":
                raise ValueError("Engine not ready")
            return {}
        checks["camunda"] = camunda
    if role == "assessment" and os.environ.get("LOANFLOW_CORE_URL"):
        async def core():
            value = await asyncio.to_thread(http_probe, os.environ["LOANFLOW_CORE_URL"] + "/internal/health/ready",
                                           os.environ.get("LOANFLOW_MONITOR_TOKEN"))
            if not value.get("ready"):
                raise ValueError("Core not ready")
            return {}
        checks["core"] = core
    results = await asyncio.gather(*(bounded(check) for check in checks.values()))
    components = dict(zip(checks, results))
    age = max(0, time.monotonic()-registry.last_cycle) if worker else 0
    return {"role": role, "ready": components["database"]["ready"],
            "workflow_ready": all(r["ready"] for r in results) and age <= 60,
            "worker_ready": age <= 60, "cycle_age_seconds": age, "components": components}


def render_snapshot(value):
    lines = []
    def metric(name, number, label=""):
        lines.append(f"loanflow_{name}{label} {number}")
    metric("ready", int(value["ready"]))
    metric("workflow_ready", int(value["workflow_ready"]))
    metric("worker_ready", int(value["worker_ready"]))
    metric("worker_cycle_age_seconds", value["cycle_age_seconds"])
    for component, item in value["components"].items():
        metric("dependency_ready", int(item["ready"]), f'{{dependency="{component}"}}')
    data = value["components"]["database"].get("values", {})
    for kind in ("outbox", "uncertain_instructions"):
        for key, number in data.get(kind, {}).items():
            metric(f"{kind}_{key}", number)
    for key in ("stalled", "scoring_overdue"):
        metric(key, data.get(key, 0))
    grouped_jobs = {}
    for row in data.get("jobs", []):
        kind = row["kind"] if row["kind"] in JOBS else "other"
        grouped = grouped_jobs.setdefault(kind, [0, 0])
        grouped[0] += row["count"]
        grouped[1] = max(grouped[1], row["oldest_seconds"])
    for kind, (count, age) in sorted(grouped_jobs.items()):
        metric("due_jobs_count", count, f'{{kind="{kind}"}}')
        metric("due_jobs_oldest_seconds", age, f'{{kind="{kind}"}}')
    for group, field, label, allowed, name in (
        ("assessments", "stage", "stage", STAGES, "assessments"),
        ("waiting", "wait_reason", "reason", ("CORE", "RETRY", "ENGINE"), "assessment_waiting"),
        ("rejections", "category", "category", ("INVALID_MESSAGE", "MESSAGE_ID_CONFLICT", "REQUEST_CONFLICT",
            "UNKNOWN_APPLICATION", "SUBMISSION_MISMATCH", "RESULT_CONFLICT"), "event_rejections"),
    ):
        counts = Counter()
        for row in data.get(group, []):
            counts[row[field] if row[field] in allowed else "other"] += row["count"]
        for key, count in sorted(counts.items()):
            metric(name, count, f'{{{label}="{key}"}}')
    if "values" in value["components"].get("kafka", {}):
        metric("kafka_lag", value["components"]["kafka"]["values"]["lag"])
    return "\n".join(lines) + "\n"


def attach(app, dsn=None, role="core", registry=None, worker=False):
    registry = registry or Registry()
    app.state.diagnostics = registry

    def denied(request):
        scheme, _, token = request.headers.get("Authorization", "").partition(" ")
        expected = os.environ.get("LOANFLOW_MONITOR_TOKEN")
        if scheme.lower() != "bearer" or not token:
            return JSONResponse({"code": "UNAUTHENTICATED"}, 401, headers={"WWW-Authenticate": "Bearer"})
        if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
            return JSONResponse({"code": "FORBIDDEN"}, 403)

    @app.get("/internal/health/live", include_in_schema=False)
    async def live(request: HttpRequest):
        return denied(request) or JSONResponse({"live": True}, headers={"Cache-Control": "no-store"})

    @app.get("/internal/health/ready", include_in_schema=False)
    async def ready(request: HttpRequest):
        if error := denied(request):
            return error
        result = await bounded(lambda: database_snapshot(dsn, role))
        return JSONResponse({"ready": result["ready"]}, 200 if result["ready"] else 503,
                            headers={"Cache-Control": "no-store"})

    @app.get("/internal/metrics", include_in_schema=False)
    async def metrics(request: HttpRequest):
        if error := denied(request):
            return error
        value = await snapshot(dsn, role, registry, worker)
        return PlainTextResponse(registry.render()+render_snapshot(value), media_type="text/plain; version=0.0.4",
                                 headers={"Cache-Control": "no-store"})

    @app.get("/internal/diagnostics", include_in_schema=False)
    async def diagnostics(request: HttpRequest):
        if error := denied(request):
            return error
        value = await snapshot(dsn, role, registry, worker)
        # JSON, как и метрики, строится по белому списку; сырые строки SQL не выходят наружу.
        return JSONResponse({"role": role, "ready": value["ready"], "workflow_ready": value["workflow_ready"],
                             "metrics": render_snapshot(value)}, headers={"Cache-Control": "no-store"})
    return registry


@contextmanager
def worker_monitor(role, dsn):
    registry = Registry()
    logger = logging.getLogger("loanflow")
    logger.addHandler(registry)
    server = thread = None
    try:
        if port := os.environ.get("LOANFLOW_MONITOR_PORT"):
            app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
            attach(app, dsn, role, registry, worker=True)
            server = uvicorn.Server(uvicorn.Config(app, host=os.environ.get("LOANFLOW_MONITOR_HOST", "127.0.0.1"),
                                                  port=int(port), access_log=False, log_level="critical"))
            thread = Thread(target=server.run, daemon=True)
            thread.start()
        yield registry
    finally:
        logger.removeHandler(registry)
        if server:
            server.should_exit = True
            thread.join(5)


def main():
    parser = argparse.ArgumentParser(description="Безопасный снимок внутреннего состояния стенда")
    parser.add_argument("--url", action="append", required=True, help="URL API или management обработчика")
    parser.add_argument("--watch", action="store_true", help="Повторять каждые 5 секунд")
    args = parser.parse_args()

    async def collect():
        async def read(url):
            async def fetch():
                return await asyncio.to_thread(http_probe, url.rstrip("/")+"/internal/diagnostics",
                                               os.environ.get("LOANFLOW_MONITOR_TOKEN"))
            return await bounded(fetch)
        # URL, секреты и сырые ошибки в вывод не попадают.
        return await asyncio.gather(*(read(url) for url in args.url))

    while True:
        started = time.monotonic()
        print(json.dumps(asyncio.run(collect()), ensure_ascii=False), flush=True)
        if not args.watch:
            break
        time.sleep(max(0, 5-(time.monotonic()-started)))


if __name__ == "__main__":
    main()
