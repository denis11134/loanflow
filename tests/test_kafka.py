"""Приёмка 5.3 на настоящем брокере и отдельных PostgreSQL."""
from copy import deepcopy
from datetime import timedelta
import json
import os
import subprocess
import sys
import time
from uuid import UUID, uuid4

from confluent_kafka import Producer
import psycopg
import pytest

from services.loanflow import assessment_service, event_bus, store
from services.loanflow.events import EventProcess
from test_service import (setup_stand, submit, update_scenario, stand_code, assert_reserve)
from test_service_abs import abs_server
from kafka_support import kafka_server

pytestmark = pytest.mark.kafka


@pytest.fixture
def broker(tmp_path_factory, request):
    bootstrap = os.environ.get("LOANFLOW_KAFKA_TEST_BOOTSTRAP")
    if bootstrap:
        yield bootstrap
        return
    if not os.environ.get("KAFKA_HOME"):
        if request.config.getoption("--require-kafka"):
            pytest.fail("Для приёмки нужен KAFKA_HOME или LOANFLOW_KAFKA_TEST_BOOTSTRAP")
        pytest.skip("Живой Kafka проверяется отдельным обязательным заданием CI")
    with kafka_server(tmp_path_factory.mktemp("kafka")) as bootstrap:
        event_bus.create_topics(bootstrap)
        yield bootstrap


@pytest.fixture
def distributed(setup_stand, broker, monkeypatch):
    stand = setup_stand
    database = "assessment_" + uuid4().hex
    params = psycopg.conninfo.conninfo_to_dict(stand.dsn)
    with psycopg.connect(stand.dsn, autocommit=True) as db:
        db.execute(psycopg.sql.SQL("CREATE DATABASE {}").format(psycopg.sql.Identifier(database)))
    params["dbname"] = database
    stand.assessment_dsn = psycopg.conninfo.make_conninfo(**params)
    assessment_service.migrate(stand.assessment_dsn)
    monkeypatch.setenv("LOANFLOW_KAFKA_BOOTSTRAP", broker)
    monkeypatch.setenv("LOANFLOW_ASSESSMENT_TOKEN", "synthetic-assessment-service-token")
    stand.group = uuid4().hex
    with stand.serve():
        monkeypatch.setenv("LOANFLOW_CORE_URL", f"http://127.0.0.1:{stand.port}")
        stand.core = EventProcess("core", stand.dsn, stand.clock, group="core-" + stand.group)
        stand.evaluator = EventProcess("assessment", stand.assessment_dsn, stand.clock, group="assessment-" + stand.group)
        try:
            yield stand
        finally:
            stand.core.close()
            stand.evaluator.close()


def assessment_sql(stand, query, params=()):
    with store.connect(stand.assessment_dsn) as db:
        cursor = db.execute(query, params)
        return cursor.fetchall() if cursor.description else None


def until(stand, condition, timeout=15):
    deadline = time.monotonic() + timeout
    while not condition():
        stand.core.run_once()
        stand.evaluator.run_once()
        if time.monotonic() > deadline:
            pytest.fail("Истёк срок ожидания распределённого результата")


def send(stand, topic, payload, key=None):
    producer = Producer({"bootstrap.servers": os.environ["LOANFLOW_KAFKA_BOOTSTRAP"], "log_level": 0})
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    key = key or payload["application_id"]
    delivered = []
    producer.produce(topic, key=key, value=value, on_delivery=lambda error, _: delivered.append(error))
    assert producer.flush(5) == 0 and delivered == [None]


def status(stand, app):
    return stand.request("GET", f"/applications/{app}", 200)["status"]


def test_live_roundtrip_duplicates_and_bad_messages(distributed):
    stand = distributed
    app = submit(stand)
    request, = stand.sql("SELECT payload FROM event_outbox")
    send(stand, event_bus.REQUEST_TOPIC, b'{"invalid":"synthetic-sensitive-marker"}', key=app)
    send(stand, event_bus.REQUEST_TOPIC, request["payload"])
    until(stand, lambda: status(stand, app) == "APPROVED")
    assert_reserve(stand, "HELD")
    assert len(stand.sql("SELECT * FROM final_decision")) == 1
    assert len(assessment_sql(stand, "SELECT * FROM assessment_task")) == 1
    result, = assessment_sql(stand, "SELECT payload FROM event_outbox")
    send(stand, event_bus.RESULT_TOPIC, result["payload"])
    until(stand, lambda: bool(assessment_sql(stand, "SELECT 1 FROM event_rejection")))
    # Другой ID с тем же бизнес-результатом также безопасен.
    repeated = deepcopy(result["payload"])
    repeated["message_id"] = str(uuid4())
    send(stand, event_bus.RESULT_TOPIC, repeated)
    until(stand, lambda: bool(stand.sql("SELECT 1 FROM event_inbox WHERE message_id=%s", (repeated["message_id"],))))
    assert len(stand.sql("SELECT * FROM final_decision")) == 1
    assert len(stand.sql("SELECT * FROM mpl_reservation")) == 1
    contradictory = deepcopy(result["payload"])
    contradictory["message_id"] = str(uuid4())
    contradictory["result"] = {"outcome": "BUSINESS_REJECTED", "reason_code": "AML_BLOCK"}
    send(stand, event_bus.RESULT_TOPIC, contradictory)
    until(stand, lambda: bool(stand.sql("SELECT 1 FROM event_rejection WHERE category='RESULT_CONFLICT'")))
    reused = deepcopy(request["payload"])
    reused["inputs"]["aml_block"] = True
    send(stand, event_bus.REQUEST_TOPIC, reused)
    until(stand, lambda: bool(assessment_sql(stand, "SELECT 1 FROM event_rejection WHERE category='MESSAGE_ID_CONFLICT'")))
    assert status(stand, app) == "APPROVED"
    diagnostics = str(assessment_sql(stand, "SELECT * FROM event_rejection"))
    assert "synthetic-sensitive-marker" not in diagnostics


@pytest.mark.parametrize(("change", "expected"), [
    ({"aml_block": True, "self_ban": True}, "BANK_REJECTED"),
    ({"consent_ref": None}, "BANK_REJECTED"),
    ({"self_ban": True}, "BANK_REJECTED"),
    ({"payroll_income_rub": None}, "BANK_REJECTED"),
    ({"payroll_income_rub": None, "existing_payments_rub": None}, "SYSTEM_ERROR"),
    ({"payroll_income_rub": "-1"}, "SYSTEM_ERROR"),
    ({"fraud_verdict": "INVALID"}, "SYSTEM_ERROR"),
])
def test_live_check_outcomes(distributed, change, expected):
    stand = distributed
    update_scenario(stand, change)
    app = submit(stand)
    until(stand, lambda: status(stand, app) == expected)
    if change.get("aml_block"):
        result, = assessment_sql(stand, "SELECT result FROM assessment_task")
        assert result["result"]["reason_code"] == "AML_BLOCK"
    if expected == "BANK_REJECTED":
        assert not stand.sql("SELECT 1 FROM mpl_reservation")


def test_live_manual_approval_uses_saved_terms(distributed):
    stand = distributed
    update_scenario(stand, {"fraud_verdict": "SUSPECTED", "payroll_income_rub": "40000.00"})
    app = submit(stand)
    until(stand, lambda: status(stand, app) == "UNDERWRITING")
    update_scenario(stand, {"payroll_income_rub": None, "quota_period": "changed"})
    receipt = stand.request("POST", f"/underwriting/applications/{app}/decision", 200,
                            "underwriter", {"command": "APPROVE"}, uuid4())
    assert receipt["application_status"] == "APPROVED"
    assert_reserve(stand, "HELD")
    until(stand, lambda: stand.request("GET", f"/applications/{app}/offer", 200)["state"] == "AVAILABLE")
    offer = stand.request("GET", f"/applications/{app}/offer", 200)
    assert offer["pdn_notice_required"] and offer["monthly_payment_minor"] == "463173"


def test_live_cancel_and_state_unavailable(distributed, monkeypatch):
    stand = distributed
    app = submit(stand)
    until(stand, lambda: bool(assessment_sql(stand, "SELECT 1 FROM assessment_task")))
    # Реальное HTTP-чтение недоступно, этап ждёт без расходования попытки.
    from kafka_support import free_port
    state = stand.evaluator.service.state
    original_url = state.url
    state.url = f"http://127.0.0.1:{free_port()}"
    stand.clock.at += timedelta(seconds=1)
    stand.evaluator.service.run_once()
    row, = assessment_sql(stand, "SELECT stage,failures,run_at FROM assessment_task")
    assert row["failures"] == 0 and row["run_at"] > stand.clock()
    state.url = original_url
    stand.request("POST", f"/applications/{app}/cancellation", 200)
    stand.clock.at += timedelta(seconds=1)
    until(stand, lambda: bool(assessment_sql(stand, "SELECT 1 FROM assessment_task WHERE stage='CANCELED'")))
    assert status(stand, app) == "CLIENT_CANCELED"
    assert not stand.sql("SELECT 1 FROM final_decision")


def test_real_processes_restart_and_disburse(distributed, monkeypatch, tmp_path):
    """API, ядро и оценка запускаются обычными командами в отдельных процессах."""
    from kafka_support import free_port

    stand = distributed
    port = free_port()
    with abs_server(stand, monkeypatch) as abs_client:
        monkeypatch.setenv("LOANFLOW_ABS_URL", abs_client.url)
        env = dict(os.environ, LOANFLOW_DSN=stand.dsn)
        processes = []

        def start(name, command, process_env):
            output = (tmp_path / f"{name}-{len(processes)}.log").open("w")
            proc = subprocess.Popen([sys.executable, *command], cwd=store.ROOT, env=process_env,
                                    stdout=output, stderr=subprocess.STDOUT)
            processes.append((proc, output))
            return proc

        try:
            start("api", ["-m", "uvicorn", "services.loanflow.api:app", "--host", "127.0.0.1",
                          "--port", str(port), "--no-access-log", "--log-level", "critical"], env)
            stand.port = port
            deadline = time.monotonic() + 15
            while True:
                try:
                    stand.request("GET", "/applications", 200)
                    break
                except OSError:
                    assert time.monotonic() < deadline
                    time.sleep(0.1)
            app = submit(stand, amount="4999900")
            request, = stand.sql("SELECT payload FROM event_outbox")
            # Подача уже закоммичена, процессы обмена ещё не запускались.
            core = start("core", ["-m", "services.loanflow.events", "core"], env)
            assessment_env = dict(env, LOANFLOW_DSN=stand.assessment_dsn)
            assessment_env.pop("LOANFLOW_ABS_TOKEN", None)
            evaluation = start("assessment", ["-m", "services.loanflow.events", "assessment"], assessment_env)

            def wait_for(predicate):
                deadline = time.monotonic() + 20
                while not predicate():
                    assert all(proc.poll() is None for proc, _ in processes)
                    assert time.monotonic() < deadline
                    time.sleep(0.1)

            wait_for(lambda: status(stand, app) == "APPROVED")
            send(stand, event_bus.REQUEST_TOPIC, request["payload"])
            # Перезапуск обоих процессов после сохранения бизнес-результата.
            for proc in (core, evaluation):
                proc.terminate()
                proc.wait(10)
            for proc, output in processes:
                if proc in (core, evaluation):
                    output.close()
            processes = [(proc, output) for proc, output in processes if proc not in (core, evaluation)]
            core = start("core-restarted", ["-m", "services.loanflow.events", "core"], env)
            start("assessment-restarted", ["-m", "services.loanflow.events", "assessment"], assessment_env)
            wait_for(lambda: stand.request("GET", f"/applications/{app}/offer", 200)["state"] == "AVAILABLE")
            offer = stand.request("GET", f"/applications/{app}/offer", 200)
            ack = f"/applications/{app}/terms/{offer['terms_id']}/pdn-notice-acknowledgement"
            if offer["pdn_notice_required"]:
                stand.request("PUT", ack, 200, body={"notice_version": offer["pdn_notice"]["version"]})
            attempt = stand.request("POST", f"/applications/{app}/signing-attempts", 201, key=uuid4())
            attempt_path = f"/applications/{app}/signing-attempts/{attempt['attempt_id']}"
            wait_for(lambda: bool(stand.sql("SELECT 1 FROM stand_sms_delivery")))
            stand.request("POST", attempt_path + "/verification", 202, key=uuid4(),
                          body={"code": stand_code(attempt["attempt_id"])})
            wait_for(lambda: bool(stand.sql("SELECT 1 FROM contract")))
            instruction, = stand.sql("SELECT instruction_id FROM disbursement_instruction")
            # При неизвестном итоговом PENDING подтверждаем АБС и читаем через сверку.
            from services.loanflow.abs import post_pending
            with store.connect(stand.dsn) as db:
                deadline = time.monotonic() + 10
                while not stand.sql("SELECT 1 FROM training_abs.instruction"):
                    assert time.monotonic() < deadline
                    time.sleep(0.1)
                post_pending(db, instruction["instruction_id"])
            stand.request("POST", f"/reconciliation/instructions/{instruction['instruction_id']}/reads",
                          202, "reconciliation", key=uuid4())
            wait_for(lambda: bool(stand.sql("SELECT 1 FROM contract WHERE status='DISBURSED'")))
            assert_reserve(stand, "CONSUMED")
            assert len(stand.sql("SELECT 1 FROM final_decision")) == 1
            assert len(assessment_sql(stand, "SELECT 1 FROM assessment_task")) == 1
            # В БД оценки отсутствуют таблицы ядра.
            assert assessment_sql(stand, "SELECT to_regclass('application') AS table_name") == [{"table_name": None}]
        finally:
            for proc, output in processes:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(5)
                output.close()


@pytest.mark.parametrize("side", ["core", "assessment"])
@pytest.mark.parametrize("boundary", ["after_event_publish", "before_event_ack", "before_event_commit"])
def test_live_loss_of_acknowledgement(distributed, side, boundary):
    stand = distributed
    app = submit(stand)
    process = stand.core if side == "core" else stand.evaluator

    class ProcessStopped(BaseException):
        pass

    crashed = []

    def stop(name, *_):
        if name == boundary and not crashed:
            crashed.append(True)
            raise ProcessStopped

    process.checkpoint = stop
    deadline = time.monotonic() + 15
    while not crashed:
        try:
            stand.core.run_once()
            stand.evaluator.run_once()
        except ProcessStopped:
            pass
        assert time.monotonic() < deadline
    process.close()
    role_dsn = stand.dsn if side == "core" else stand.assessment_dsn
    restarted = EventProcess(side, role_dsn, stand.clock, group=side + "-" + stand.group)
    if side == "core":
        stand.core = restarted
    else:
        stand.evaluator = restarted
    until(stand, lambda: status(stand, app) == "APPROVED")
    assert len(stand.sql("SELECT 1 FROM final_decision")) == 1
    assert len(stand.sql("SELECT 1 FROM mpl_reservation")) == 1
    assert len(assessment_sql(stand, "SELECT 1 FROM assessment_task")) == 1


def test_cancel_during_external_call_and_late_result(distributed):
    stand = distributed
    app = submit(stand)
    until(stand, lambda: bool(assessment_sql(stand, "SELECT 1 FROM assessment_task WHERE stage='PASSPORT'")))
    completed_calls = []
    original = stand.evaluator.service.checks.call

    def cancel_after_call(*args):
        result = original(*args)
        stand.request("POST", f"/applications/{app}/cancellation", 200)
        completed_calls.append(args[0])
        return result

    stand.evaluator.service.checks.call = cancel_after_call
    stand.evaluator.service.run_once()
    stand.evaluator.service.run_once()
    assert completed_calls == ["PASSPORT"]
    assert assessment_sql(stand, "SELECT stage FROM assessment_task") == [{"stage": "CANCELED"}]
    # Все виды окончательного результата, пришедшие после отмены, безопасны.
    for result in ({"outcome": "BUSINESS_REJECTED", "reason_code": "SELF_BAN"},
                   {"outcome": "TECHNICAL_FAILURE", "failure_kind": "PERMANENT"}):
        payload = {"message_id": str(uuid4()), "application_id": app,
                   "submitted_at": stand.sql("SELECT submitted_at FROM application")[0]["submitted_at"].isoformat(),
                   "result": result}
        send(stand, event_bus.RESULT_TOPIC, payload)
        until(stand, lambda: bool(stand.sql("SELECT 1 FROM event_inbox WHERE message_id=%s", (payload["message_id"],))))
    assert status(stand, app) == "CLIENT_CANCELED" and not stand.sql("SELECT 1 FROM final_decision")


def test_third_aml_timeout_survives_restart(distributed):
    stand = distributed
    app = submit(stand)
    # Сохраняем запрос, пока состояние ядра временно недоступно.
    from kafka_support import free_port
    original_url = stand.evaluator.service.state.url
    stand.evaluator.service.state.url = f"http://127.0.0.1:{free_port()}"
    until(stand, lambda: bool(assessment_sql(stand, "SELECT 1 FROM assessment_task")))
    for attempt in (1, 2, 3):
        assessment_sql(stand, "INSERT INTO training_assessment_fault VALUES (%s,'AML',%s,'TEMPORARY')", (app, attempt))
    stand.evaluator.service.state.url = original_url
    stand.clock.at += timedelta(seconds=1)
    assert stand.evaluator.service.run_once()
    row, = assessment_sql(stand, "SELECT stage,failures,run_at FROM assessment_task")
    assert row["failures"] == 1 and row["run_at"] == stand.clock() + timedelta(seconds=1)
    stand.evaluator.close()
    stand.evaluator = EventProcess("assessment", stand.assessment_dsn, stand.clock, group="assessment-" + stand.group)
    stand.clock.at += timedelta(seconds=1)
    assert stand.evaluator.service.run_once()
    assert assessment_sql(stand, "SELECT failures FROM assessment_task") == [{"failures": 2}]
    stand.clock.at += timedelta(seconds=3)
    until(stand, lambda: status(stand, app) == "SYSTEM_ERROR")
    assert assessment_sql(stand, "SELECT result FROM assessment_task") == [{
        "result": {"outcome": "TECHNICAL_FAILURE", "failure_kind": "RETRIES_EXHAUSTED"},
    }]


def test_internal_endpoint_separates_service_credentials(distributed):
    from http.client import HTTPConnection

    stand = distributed
    app = submit(stand)
    path = f"/internal/applications/{app}/scoring-state"
    for headers, expected in (
        ({}, 401),
        ({"Authorization": "Bearer " + stand.tokens["mobile"]}, 403),
        ({"Authorization": "Bearer synthetic-abs-token"}, 403),
        ({"Authorization": "Bearer synthetic-assessment-service-token"}, 200),
    ):
        conn = HTTPConnection("127.0.0.1", stand.port)
        try:
            conn.request("GET", path, headers=headers)
            response = conn.getresponse()
            body = json.loads(response.read())
            assert response.status == expected
            if expected == 200:
                assert set(body) == {"application_id", "submitted_at", "status"}
                assert body["status"] == "SCORING"
        finally:
            conn.close()
    assert stand.request("GET", "/applications", 401, headers={
        "Authorization": "Bearer synthetic-assessment-service-token",
    })["code"] == "UNAUTHENTICATED"


def test_live_quota_and_request_snapshot(distributed):
    stand = distributed
    stand.sql("UPDATE mpl_quota SET capacity=1")
    first, second = submit(stand), submit(stand, "other")
    update_scenario(stand, {"payroll_income_rub": None, "quota_period": "changed"})
    until(stand, lambda: status(stand, first) in ("APPROVED", "BANK_REJECTED")
          and stand.request("GET", f"/applications/{second}", 200, "other")["status"] in ("APPROVED", "BANK_REJECTED"))
    decisions = stand.sql("SELECT outcome,rejection_reason_code FROM final_decision")
    assert sorted(row["outcome"] for row in decisions) == ["APPROVED", "REJECTED"]
    assert next(row["rejection_reason_code"] for row in decisions if row["outcome"] == "REJECTED") == "MPL_QUOTA_EXHAUSTED"
    assert len(stand.sql("SELECT 1 FROM mpl_reservation WHERE state='HELD'")) == 1


@pytest.mark.parametrize("offset", [0, 1])
def test_evaluator_exact_pdn_boundary(offset):
    inputs = {
        "requested_amount_minor": "5000000", "requested_term_months": 12,
        "payroll_income": {"state": "AVAILABLE", "amount_minor": "10000000"},
        "quota_ref": {"period": "demo", "bucket": "standard"},
    }
    result, _ = assessment_service.calculate(inputs, 5_000_000 - 463173 + offset)
    assert result["pdn_notice_required"] is bool(offset)


@pytest.mark.parametrize(("payment", "exceeded"), [(122499, False), (122500, False), (122501, True)])
def test_evaluator_exact_psk_boundary(monkeypatch, payment, exceeded):
    from services.loanflow import calculations

    inputs = {
        "requested_amount_minor": "120000", "requested_term_months": 1,
        "payroll_income": {"state": "AVAILABLE", "amount_minor": "10000000"},
        "quota_ref": {"period": "demo", "bucket": "standard"},
    }
    monkeypatch.setattr(calculations, "annuity_schedule", lambda *_: (payment, [payment], payment - 120000))
    result, _ = assessment_service.calculate(inputs, 0)
    assert result["psk_limit_exceeded"] is exceeded


def test_core_jobs_continue_when_kafka_fails():
    """Сбой транспорта не должен подавить независимые операции ядра."""
    from types import SimpleNamespace

    calls = []

    def unavailable(*_):
        raise TimeoutError("synthetic-transport-error")

    process = EventProcess.__new__(EventProcess)
    process.role = "core"
    process.dsn = "unused"
    process.topic = event_bus.RESULT_TOPIC
    process.clock = store.now
    process.checkpoint = lambda *_: None
    process.bus = SimpleNamespace(consume_one=unavailable, publish_one=unavailable)
    process.service = SimpleNamespace(run_once=lambda: calls.append("core") or True)
    assert process.run_once()
    assert calls == ["core"]


@pytest.mark.parametrize(("code", "payload", "expected"), [
    (404, {"code": "NOT_FOUND"}, False),
    (404, {"other": "synthetic-sensitive-marker"}, None),
    (503, {"code": "SERVICE_UNAVAILABLE"}, None),
    (200, {"application_id": "20000000-0000-4000-8000-000000000001",
           "submitted_at": "2026-11-17T08:00:00", "status": "SCORING"}, None),
])
def test_state_reader_requires_authoritative_response(monkeypatch, code, payload, expected):
    from datetime import datetime, timezone
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from test_service import http_server

    app = FastAPI()
    app.add_api_route("/internal/applications/{application_id}/scoring-state",
                      lambda: JSONResponse(payload, status_code=code))
    monkeypatch.setenv("LOANFLOW_ASSESSMENT_TOKEN", "synthetic-state-reader-token")
    with http_server(app) as port:
        client = assessment_service.CoreStateClient(f"http://127.0.0.1:{port}")
        assert client.active({
            "application_id": uuid4() if code != 200 else
                UUID("20000000-0000-4000-8000-000000000001"),
            "submitted_at": datetime(2026, 11, 17, 8, tzinfo=timezone.utc),
        }) is expected
