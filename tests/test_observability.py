"""Диагностика не раскрывает данные и не блокирует обычные запросы."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
import logging
import time
from uuid import uuid4

from services.loanflow import assessment_service, observability as obs
from services.loanflow.api import create_app
from test_service import setup_stand, http_server


def get(port, path, token=None):
    connection = HTTPConnection("127.0.0.1", port, timeout=4)
    try:
        connection.request("GET", path, headers={"Authorization": f"Bearer {token}"} if token else {})
        response = connection.getresponse()
        return response.status, response.read().decode()
    finally:
        connection.close()


def test_internal_auth_redaction_and_readiness(setup_stand, monkeypatch):
    stand = setup_stand
    monkeypatch.setenv("LOANFLOW_MONITOR_TOKEN", "synthetic-monitor-secret")
    monkeypatch.setenv("LOANFLOW_ASSESSMENT_TOKEN", "synthetic-assessment-secret")
    app = create_app(stand.dsn, stand.clock)
    with http_server(app) as port:
        for path in ("/internal/health/live", "/internal/health/ready", "/internal/metrics", "/internal/diagnostics"):
            assert get(port, path)[0] == 401
            for token in (*stand.tokens.values(), "synthetic-assessment-secret", "synthetic-abs-secret"):
                assert get(port, path, token)[0] == 403
            assert get(port, path, "synthetic-monitor-secret")[0] == 200
        assert get(port, "/internal/health/ready", "synthetic-monitor-secret")[1] == '{"ready":true}'
        for _ in range(20):
            get(port, "/secret-marker-"+uuid4().hex+"?token=synthetic-payload-secret")
        code, metrics = get(port, "/internal/metrics", "synthetic-monitor-secret")
        assert code == 200
        for secret in ("synthetic-", "secret-marker", str(stand.tokens), "application_id", "customer_id"):
            assert secret not in metrics
        assert 'operation="unmatched"' in metrics
        assert len(app.state.http_metrics.samples) <= 5
    with http_server(create_app("host=/nonexistent dbname=absent")) as port:
        started = time.monotonic()
        assert get(port, "/internal/health/ready", "synthetic-monitor-secret")[0] == 503
        assert time.monotonic()-started < 2


def test_slow_diagnostics_do_not_block_business_api(setup_stand, monkeypatch):
    stand = setup_stand
    monkeypatch.setenv("LOANFLOW_MONITOR_TOKEN", "synthetic-monitor")

    async def unavailable(*_):
        await asyncio.sleep(5)

    monkeypatch.setattr(obs, "database_snapshot", unavailable)
    with stand.serve():
        with ThreadPoolExecutor(1) as pool:
            started = time.monotonic()
            request = pool.submit(get, stand.port, "/internal/metrics", "synthetic-monitor")
            time.sleep(.1)
            assert stand.request("GET", "/applications", 200) == []
            assert time.monotonic()-started < 1
            code, body = request.result(3)
            assert code == 200 and 'dependency="database"} 0' in body
            assert time.monotonic()-started < 2.5


def test_stall_and_wait_are_distinct_and_progress_is_durable(setup_stand):
    stand = setup_stand
    assessment_service.migrate(stand.dsn)
    app = uuid4()
    stand.sql("""INSERT INTO assessment_task
        (application_id,submitted_at,request_fingerprint,inputs,stage,run_at,engine,progress_at)
        VALUES (%s,now()-interval '200 seconds','x','{}','AML',now(),'camunda',now()-interval '70 seconds')""", (app,))

    def snapshot():
        return asyncio.run(obs.database_snapshot(stand.dsn, "assessment"))

    assert snapshot()["stalled"] == 1
    stand.sql("UPDATE assessment_task SET wait_reason='RETRY',wait_until=now()+interval '3 seconds'")
    assert snapshot()["stalled"] == 0
    stand.sql("UPDATE assessment_task SET wait_reason='CORE',wait_until=now()+interval '1 second'")
    assert snapshot()["stalled"] == 1
    with obs.store.connect(stand.dsn) as db:
        before = obs.store.one(db, "SELECT progress_at FROM assessment_task")["progress_at"]
        assessment_service.progress(db, {"application_id": app}, obs.store.now(), "CORE", advanced=False)
        assert obs.store.one(db, "SELECT progress_at FROM assessment_task")["progress_at"] == before
    # Незапущенный срок исполнения Camunda не используется как признак продвижения.
    stand.sql("UPDATE assessment_task SET run_at=now()+interval '1 day'")
    assert snapshot()["stalled"] == 1
    stand.sql("UPDATE assessment_task SET stage='COMPLETED'")
    assert snapshot()["stalled"] == 0


def test_future_jobs_and_uncertain_instructions(setup_stand):
    stand = setup_stand
    stand.sql("INSERT INTO service_job(kind,resource_id,run_at) VALUES ('COOLING',%s,now()+interval '4 hours')", (uuid4(),))
    stand.sql("INSERT INTO service_job(kind,resource_id,run_at) VALUES ('SMS',%s,now()-interval '5 seconds')", (uuid4(),))
    data = asyncio.run(obs.database_snapshot(stand.dsn, "core"))
    assert len(data["jobs"]) == 1 and data["jobs"][0]["kind"] == "SMS"
    assert data["uncertain_instructions"]["count"] == 0


def test_http_metrics_include_final_body_and_bounded_labels():
    registry = obs.Registry(["readApplication"])

    async def app(scope, receive, send):
        scope["route"] = type("Route", (), {"name": "readApplication"})()
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"first", "more_body": True})
        await asyncio.sleep(.03)
        await send({"type": "http.response.body", "body": b"last"})

    async def run():
        async def send(_):
            pass
        await obs.HttpMetrics(app, registry)({"type": "http"}, None, send)

    asyncio.run(run())
    assert registry.samples[("readApplication", "2xx")][0] == 1
    assert registry.samples[("readApplication", "2xx")][1] >= .03
    for _ in range(20):
        registry.observe(str(uuid4()), 404, .01)
        registry.emit(logging.LogRecord("loanflow", logging.ERROR, "", 0, "secret-message-%s", (uuid4(),), None))
    assert len(registry.samples) == 2
    assert registry.errors["other"] == 20 and sum(registry.errors.values()) == 20
    assert "secret" not in registry.render()


def test_started_instruction_is_visible_without_resending(setup_stand, monkeypatch):
    import pytest
    from test_service import signed_contract, worker, crash_at, SimulatedProcessStop
    from test_service_abs import abs_server, instruction_id, calls

    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        with pytest.raises(SimulatedProcessStop):
            worker(stand, abs_client=client, checkpoint=crash_at("after_started_commit")).run_once("SEND")
        data = asyncio.run(obs.database_snapshot(stand.dsn, "core"))
        assert data["uncertain_instructions"]["count"] == 1
        assert not worker(stand, abs_client=client).run_once("SEND")
        assert calls(stand, instruction, "SEND") == 0
