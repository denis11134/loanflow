"""Приёмка 5.4: настоящий Camunda 8, Kafka и отдельные PostgreSQL."""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
import subprocess
import sys
import time
from uuid import uuid4

import pytest
from psycopg import sql

from services.loanflow import assessment_service, calculations, event_bus, store
from services.loanflow.events import EventProcess
from services.loanflow.orchestration import CamundaError
from camunda_support import camunda_configured, camunda_server
from kafka_support import free_port
from test_kafka import (
    assessment_sql, broker as kafka_broker, distributed as kafka_distributed,
    send, status, until as kafka_until,
    test_live_roundtrip_duplicates_and_bad_messages as kafka_duplicates,
    test_live_quota_and_request_snapshot as kafka_quota,
    test_real_processes_restart_and_disburse as kafka_disbursement,
)
from test_service import setup_stand, submit, update_scenario, assert_reserve, stand_code
from test_service_abs import abs_server

pytestmark = pytest.mark.camunda


def until(stand, condition, timeout=15):
    try:
        return kafka_until(stand, condition, timeout)
    except pytest.fail.Exception:
        # Только технический ход оценки; входы и результаты проверок не печатаются.
        try:
            rows = assessment_sql(stand, """SELECT application_id,engine,stage,failures,process_instance_key
                                           FROM assessment_task ORDER BY application_id""")
            print("assessment_progress:", store.as_json(rows))
        except Exception as error:
            print("assessment_progress_unavailable:", type(error).__name__)
        raise


@pytest.fixture
def broker(tmp_path_factory, request):
    # Новый журнал: consumer групп оценки читает с earliest, включая старые заявки.
    yield from kafka_broker.__wrapped__(tmp_path_factory, request)


class Engine:
    def __init__(self, directory):
        self.directory = directory
        self.context = None

    def start(self):
        context = camunda_server(self.directory)
        self.url = context.__enter__()
        self.context = context

    def stop(self):
        if self.context is not None:
            context, self.context = self.context, None
            context.__exit__(None, None, None)

    def restart(self):
        self.stop()
        self.start()


@pytest.fixture(scope="module")
def engine(tmp_path_factory, request):
    if not camunda_configured():
        if request.config.getoption("--require-camunda"):
            pytest.fail("Для приёмки нужен CAMUNDA_HOME или LOANFLOW_CAMUNDA_TEST_URL")
        pytest.skip("Живой Camunda проверяется отдельным обязательным заданием CI")
    instance = Engine(tmp_path_factory.mktemp("camunda"))
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()


@pytest.fixture
def distributed(engine, setup_stand, broker, monkeypatch):
    monkeypatch.setenv("LOANFLOW_ASSESSMENT_ENGINE", "camunda")
    monkeypatch.setenv("LOANFLOW_CAMUNDA_URL", engine.url)
    monkeypatch.setenv("LOANFLOW_CAMUNDA_JOB_TIMEOUT_MS", "2000")
    yield from kafka_distributed.__wrapped__(setup_stand, broker, monkeypatch)


def restart(stand):
    stand.evaluator.close()
    stand.evaluator = EventProcess("assessment", stand.assessment_dsn, stand.clock,
                                   group="assessment-" + stand.group)
    return stand.evaluator.service


def task(stand, app):
    rows = assessment_sql(stand, "SELECT * FROM assessment_task WHERE application_id=%s", (app,))
    return rows[0] if rows else None


def wait_for_stage(stand, app, stage):
    until(stand, lambda: bool(task(stand, app)) and task(stand, app)["stage"] == stage, timeout=30)


def assert_single_result(stand, app):
    assert len(assessment_sql(stand, "SELECT 1 FROM event_outbox WHERE application_id=%s", (app,))) == 1
    assert len(stand.sql("SELECT 1 FROM final_decision WHERE application_id=%s", (app,))) == 1
    assert len(stand.sql("SELECT 1 FROM mpl_reservation JOIN offer USING (offer_id) WHERE application_id=%s", (app,))) == 1
    assert task(stand, app)["engine"] == "camunda"
    assert task(stand, app)["process_instance_key"]


class ProcessStopped(BaseException):
    """Имитируем остановку процесса, обходящую обработчик обычных ошибок."""


def stop_once(service, boundary, stage=None):
    stopped = []

    def checkpoint(name, row=None, *_):
        current_stage = row.get("stage") if row else None
        if row and "type" in row:
            current_stage = row["type"].removeprefix("loanflow-assessment-")
            if current_stage == "finalize":
                current_stage = "RESULT_READY"
        if name == boundary and not stopped and (stage is None or current_stage == stage):
            stopped.append(True)
            raise ProcessStopped

    service.checkpoint = checkpoint
    return stopped


def run_until_stopped(stand, stopped, timeout=30):
    deadline = time.monotonic() + timeout
    while not stopped:
        try:
            stand.core.run_once()
            stand.evaluator.run_once()
        except ProcessStopped:
            pass
        assert time.monotonic() < deadline, "Не достигнута граница остановки процесса оценки"


@pytest.mark.parametrize(("scenario", "expected", "reason"), [
    ({}, "APPROVED", None),
    ({"aml_block": True, "self_ban": True}, "BANK_REJECTED", "AML_BLOCK"),
    ({"passport_verdict": "INVALID", "consent_ref": None}, "BANK_REJECTED", "PASSPORT_INVALID"),
    ({"consent_ref": None}, "BANK_REJECTED", "CREDIT_REPORT_CONSENT_REQUIRED"),
    ({"self_ban": True}, "BANK_REJECTED", "SELF_BAN"),
    ({"payroll_income_rub": None}, "BANK_REJECTED", "NO_CONFIRMED_INCOME"),
    ({"payroll_income_rub": "0"}, "BANK_REJECTED", "NO_CONFIRMED_INCOME"),
    ({"payroll_income_rub": "40000.00"}, "BANK_REJECTED", "SCORING_BELOW_THRESHOLD"),
    ({"payroll_income_rub": "-1"}, "SYSTEM_ERROR", None),
    ({"fraud_verdict": "INVALID"}, "SYSTEM_ERROR", None),
    ({"payroll_income_rub": None, "existing_payments_rub": None}, "SYSTEM_ERROR", None),
])
def test_camunda_business_outcomes(distributed, scenario, expected, reason):
    stand = distributed
    update_scenario(stand, scenario)
    app = submit(stand)
    until(stand, lambda: status(stand, app) == expected, timeout=30)
    assert task(stand, app)["engine"] == "camunda"
    assert task(stand, app)["process_instance_key"]
    if reason:
        assert stand.sql("SELECT rejection_reason_code FROM final_decision") == [
            {"rejection_reason_code": reason}]
        assert not stand.sql("SELECT 1 FROM mpl_reservation")
    elif expected == "APPROVED":
        assert_reserve(stand, "HELD")
        assert_single_result(stand, app)
    else:
        assert not stand.sql("SELECT 1 FROM final_decision")


@pytest.mark.parametrize(("scenario", "term"), [
    ({"fraud_verdict": "SUSPECTED", "payroll_income_rub": "40000.00"}, 12),
    ({"payroll_income_rub": "50000.00"}, 24),
], ids=["suspected-fraud", "scoring-manual"])
def test_camunda_manual_approval_uses_saved_terms(distributed, scenario, term):
    stand = distributed
    update_scenario(stand, scenario)
    app = submit(stand, term=term)
    until(stand, lambda: status(stand, app) == "UNDERWRITING", timeout=30)
    assert not stand.sql("SELECT 1 FROM mpl_reservation")
    update_scenario(stand, {"payroll_income_rub": None, "quota_period": "changed"})
    decision = stand.request("POST", f"/underwriting/applications/{app}/decision", 200,
                             "underwriter", {"command": "APPROVE"}, uuid4())
    assert decision["application_status"] == "APPROVED"
    until(stand, lambda: stand.request("GET", f"/applications/{app}/offer", 200)["state"] == "AVAILABLE")
    payment = calculations.annuity_schedule(5_000_000, term, calculations.MONTHLY_RATE)[0]
    assert stand.request("GET", f"/applications/{app}/offer", 200)["monthly_payment_minor"] == str(payment)
    assert_reserve(stand, "HELD")


@pytest.mark.parametrize(("offset", "required"), [(0, False), (1, True)])
def test_camunda_exact_pdn_boundary(distributed, offset, required):
    stand = distributed
    existing = 5_000_000 - 463173 + offset
    update_scenario(stand, {"payroll_income_rub": "100000.00",
                            "existing_payments_rub": f"{existing // 100}.{existing % 100:02d}"})
    app = submit(stand)
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    until(stand, lambda: stand.request("GET", f"/applications/{app}/offer", 200)["state"] == "AVAILABLE")
    assert stand.request("GET", f"/applications/{app}/offer", 200)["pdn_notice_required"] is required


@pytest.mark.parametrize(("payment", "expected"), [(122499, "APPROVED"), (122500, "APPROVED"),
                                                        (122501, "BANK_REJECTED")])
def test_camunda_exact_psk_boundary(distributed, monkeypatch, payment, expected):
    stand = distributed
    monkeypatch.setattr(calculations, "annuity_schedule", lambda *_: (payment, [payment], payment - 120000))
    app = submit(stand, amount="120000", term=1)
    until(stand, lambda: status(stand, app) == expected, timeout=30)
    if expected == "BANK_REJECTED":
        assert stand.sql("SELECT rejection_reason_code FROM final_decision") == [
            {"rejection_reason_code": "PSK_LIMIT_EXCEEDED"}]


def test_camunda_quota_and_snapshot(distributed):
    kafka_quota(distributed)


def test_camunda_kafka_duplicates_and_conflicts(distributed):
    kafka_duplicates(distributed)


@pytest.mark.parametrize("recover", [False, True], ids=["third-timeout", "third-success"])
def test_camunda_durable_one_and_three_second_timers(distributed, recover):
    stand = distributed
    # Приём запроса не должен успеть выполнить AML до установки синтетического сбоя.
    app = submit(stand)
    service = stand.evaluator.service
    original_state = service.state.active
    service.state.active = lambda _: None
    until(stand, lambda: task(stand, app) is not None, timeout=30)
    for attempt in range(1, 3 if recover else 4):
        assessment_sql(stand, "INSERT INTO training_assessment_fault VALUES (%s,'AML',%s,'TEMPORARY')", (app, attempt))
    service.state.active = original_state
    # Бизнес-часы не двигаются: ожидание должно исполняться самим движком.
    frozen = stand.clock()
    started = time.monotonic()
    until(stand, lambda: task(stand, app)["failures"] == 1, timeout=30)
    first = time.monotonic()
    assert task(stand, app)["stage"] == "AML"
    restart(stand)
    until(stand, lambda: task(stand, app)["failures"] == 2, timeout=30)
    second = time.monotonic()
    assert second - first >= 0.75
    restart(stand)
    until(stand, lambda: status(stand, app) == ("APPROVED" if recover else "SYSTEM_ERROR"), timeout=30)
    assert time.monotonic() - second >= 2.75
    assert time.monotonic() - started >= 3.75
    assert stand.clock() == frozen
    steps = assessment_sql(stand, "SELECT attempt FROM assessment_step WHERE application_id=%s AND stage='AML' ORDER BY attempt", (app,))
    assert steps == [{"attempt": n} for n in (1, 2, 3)]
    if not recover:
        result = task(stand, app)["result"]
        assert "RETRIES_EXHAUSTED" in json.dumps(result)


def test_camunda_engine_restart_preserves_waiting_timer(distributed, engine, monkeypatch):
    if os.environ.get("LOANFLOW_CAMUNDA_TEST_URL"):
        pytest.skip("Перезапуск внешнего Camunda выполняет его владелец; CI запускает локальный Run")
    stand = distributed
    app = submit(stand)
    service = stand.evaluator.service
    original_state = service.state.active
    service.state.active = lambda _: None
    until(stand, lambda: task(stand, app) is not None, timeout=30)
    assessment_sql(stand, "INSERT INTO training_assessment_fault VALUES (%s,'AML',1,'TEMPORARY')", (app,))
    service.state.active = original_state
    stopped = stop_once(service, "after_camunda_complete", "AML")
    run_until_stopped(stand, stopped)
    saved = task(stand, app)
    assert saved["failures"] == 1
    frozen = stand.clock()
    # H2 и журнал Zeebe остаются в том же каталоге; процесс Java действительно завершается.
    engine.restart()
    monkeypatch.setenv("LOANFLOW_CAMUNDA_URL", engine.url)
    restart(stand)
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    assert stand.clock() == frozen
    assert task(stand, app)["process_instance_key"] == saved["process_instance_key"]
    assert assessment_sql(stand, "SELECT attempt FROM assessment_step WHERE application_id=%s AND stage='AML' ORDER BY attempt", (app,)) == [
        {"attempt": 1}, {"attempt": 2}]
    assert_single_result(stand, app)


def test_camunda_state_unavailable_does_not_spend_attempt(distributed):
    stand = distributed
    app = submit(stand)
    service = stand.evaluator.service
    original_url = service.state.url
    service.state.url = f"http://127.0.0.1:{free_port()}"
    until(stand, lambda: task(stand, app) is not None, timeout=30)
    deadline = time.monotonic() + 2.2
    while time.monotonic() < deadline:
        stand.evaluator.run_once()
    assert task(stand, app)["failures"] == 0
    assert not assessment_sql(stand, "SELECT 1 FROM assessment_step WHERE application_id=%s AND stage IN ('AML','PASSPORT')", (app,))
    assert status(stand, app) == "SCORING"
    service.state.url = original_url
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)


@pytest.mark.parametrize("during_call", [False, True], ids=["between-stages", "during-check"])
def test_camunda_cancellation_gates_external_calls_and_late_results(distributed, during_call):
    stand = distributed
    app = submit(stand)
    wait_for_stage(stand, app, "PASSPORT")
    service = stand.evaluator.service
    calls = []
    original = service.checks.call

    def check(*args):
        calls.append(args[0])
        result = original(*args)
        if during_call:
            stand.request("POST", f"/applications/{app}/cancellation", 200)
        return result

    service.checks.call = check
    if not during_call:
        stand.request("POST", f"/applications/{app}/cancellation", 200)
    wait_for_stage(stand, app, "CANCELED")
    assert calls == (["PASSPORT"] if during_call else [])
    assert not assessment_sql(stand, "SELECT 1 FROM event_outbox WHERE application_id=%s", (app,))
    submitted = stand.sql("SELECT submitted_at FROM application WHERE application_id=%s", (app,))[0]["submitted_at"]
    for result in ({"outcome": "BUSINESS_REJECTED", "reason_code": "SELF_BAN"},
                   {"outcome": "TECHNICAL_FAILURE", "failure_kind": "PERMANENT"}):
        payload = {"message_id": str(uuid4()), "application_id": app,
                   "submitted_at": submitted.isoformat(), "result": result}
        send(stand, event_bus.RESULT_TOPIC, payload)
        until(stand, lambda: bool(stand.sql("SELECT 1 FROM event_inbox WHERE message_id=%s", (payload["message_id"],))))
    assert status(stand, app) == "CLIENT_CANCELED"
    assert not stand.sql("SELECT 1 FROM final_decision")


@pytest.mark.parametrize("boundary", ["before_camunda_start", "after_camunda_start"])
def test_camunda_lost_creation_response_is_recoverable(distributed, boundary):
    stand = distributed
    app = submit(stand)
    stopped = stop_once(stand.evaluator.service, boundary)
    run_until_stopped(stand, stopped)
    if boundary == "after_camunda_start":
        # Business ID защищает только активный экземпляр; REGISTER ещё не выполнен.
        with pytest.raises(CamundaError) as conflict:
            stand.evaluator.service.client.create(task(stand, app))
        assert conflict.value.status == 409
    restart(stand)
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    assert_single_result(stand, app)
    steps = assessment_sql(stand, "SELECT stage,attempt,count(*) AS n FROM assessment_step WHERE application_id=%s GROUP BY stage,attempt", (app,))
    assert all(row["n"] == 1 for row in steps)


@pytest.mark.parametrize("corruption", ["null", "leading-zero"])
def test_camunda_malformed_creation_response_does_not_bind_wrong_instance(distributed, corruption):
    stand = distributed
    service = stand.evaluator.service
    original = service.client.create
    corrupted = []

    def invalid_response(row):
        response = original(row)
        if not corrupted:
            corrupted.append(True)
            key = None if corruption == "null" else "0" + str(response["processInstanceKey"])
            return dict(response, processInstanceKey=key)
        return response

    service.client.create = invalid_response
    app = submit(stand)
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    assert corrupted
    assert task(stand, app)["process_instance_key"].isdecimal()
    assert_single_result(stand, app)


def engine_completed(client, key):
    try:
        return client.request("GET", f"/process-instances/{key}")["state"] == "COMPLETED"
    except CamundaError as exc:
        if exc.status == 404:  # Проекция REST появляется асинхронно.
            return False
        raise


def test_camunda_new_instance_after_completion_is_stopped_by_database(distributed):
    stand = distributed
    app = submit(stand)
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    client = stand.evaluator.service.client
    saved = task(stand, app)
    until(stand, lambda: engine_completed(client, saved["process_instance_key"]), timeout=30)
    baseline = assessment_sql(stand, "SELECT * FROM assessment_step WHERE application_id=%s ORDER BY stage,attempt", (app,))
    result, = assessment_sql(stand, "SELECT payload FROM event_outbox WHERE application_id=%s", (app,))
    responses = []
    original_complete = client.complete

    def capture(key, variables):
        responses.append(deepcopy(variables))
        return original_complete(key, variables)

    client.complete = capture
    repeated = client.create(saved)
    assert str(repeated["processInstanceKey"]) != saved["process_instance_key"]
    until(stand, lambda: engine_completed(client, repeated["processInstanceKey"]), timeout=30)
    assert responses == [{"route": "STOP"}]
    assert task(stand, app)["process_instance_key"] == saved["process_instance_key"]
    assert assessment_sql(stand, "SELECT * FROM assessment_step WHERE application_id=%s ORDER BY stage,attempt", (app,)) == baseline
    assert assessment_sql(stand, "SELECT payload FROM event_outbox WHERE application_id=%s", (app,)) == [result]
    assert_single_result(stand, app)


@pytest.mark.parametrize("boundary", [
    "before_assessment_step", "after_assessment_step", "before_assessment_commit",
    "after_camunda_step_commit", "before_camunda_complete", "after_camunda_complete",
])
def test_camunda_step_replay_after_process_crash(distributed, boundary):
    stand = distributed
    app = submit(stand)
    calls = []
    service = stand.evaluator.service
    original = service.checks.call

    def counted(*args):
        calls.append(args[0])
        return original(*args)

    service.checks.call = counted
    stopped = stop_once(service, boundary, "PASSPORT")
    run_until_stopped(stand, stopped)
    committed = boundary in {"after_camunda_step_commit", "before_camunda_complete", "after_camunda_complete"}
    service = restart(stand)
    original = service.checks.call
    service.checks.call = lambda *args: (calls.append(args[0]), original(*args))[1]
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    assert calls.count("PASSPORT") == (2 if boundary in {"after_assessment_step", "before_assessment_commit"} else 1)
    if committed:
        assert Counter(calls) == Counter({"PASSPORT": 1, "CREDIT_REPORT": 1, "FRAUD": 1})
    assert_single_result(stand, app)
    assert len(assessment_sql(stand, "SELECT 1 FROM assessment_step WHERE application_id=%s AND stage='PASSPORT'", (app,))) == 1


def test_camunda_finalization_replay_is_exactly_once(distributed):
    stand = distributed
    app = submit(stand)
    stopped = stop_once(stand.evaluator.service, "after_camunda_step_commit", "RESULT_READY")
    run_until_stopped(stand, stopped)
    saved, = assessment_sql(stand, "SELECT payload FROM event_outbox WHERE application_id=%s", (app,))
    restart(stand)
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    # Повторная Kafka-доставка завершённой оценки не создаёт вторую рекомендацию.
    request, = stand.sql("SELECT payload FROM event_outbox WHERE application_id=%s", (app,))
    repeated = deepcopy(request["payload"])
    repeated["message_id"] = str(uuid4())
    send(stand, event_bus.REQUEST_TOPIC, repeated)
    until(stand, lambda: bool(assessment_sql(stand, "SELECT 1 FROM event_inbox WHERE message_id=%s", (repeated["message_id"],))))
    assert_single_result(stand, app)
    assert assessment_sql(stand, "SELECT payload FROM event_outbox WHERE application_id=%s", (app,)) == [saved]


def test_camunda_variables_contain_only_routing_data(distributed):
    stand = distributed
    requests = []
    client = stand.evaluator.service.client
    original = client.request

    def capture(method, path, body=None, *args):
        if isinstance(body, dict) and "variables" in body:
            requests.append(deepcopy(body["variables"]))
        return original(method, path, body, *args)

    client.request = capture
    app = submit(stand)
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    assert requests
    assert any(variables.get("assessmentId") == app for variables in requests)
    for variables in requests:
        assert set(variables) <= {"assessmentId", "route", "waitDuration"}
        assert all(value in {app, "NEXT", "FINISH", "STOP", "WAIT", "PT1S", "PT3S"}
                   for value in variables.values())


def test_camunda_outage_does_not_block_core_publication(distributed):
    stand = distributed
    app = submit(stand)
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    client = stand.evaluator.service.client
    original = client.request
    original_url = client.url
    client.url = f"http://127.0.0.1:{free_port()}"
    calls = []

    def unavailable(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    client.request = unavailable
    until(stand, lambda: stand.request("GET", f"/applications/{app}/offer", 200)["state"] == "AVAILABLE")
    # Независимое чтение/отмена и таймер предложения остаются в ядре.
    stand.clock.at += timedelta(days=10)
    until(stand, lambda: status(stand, app) == "OFFER_EXPIRED")
    assert calls
    assert_reserve(stand, "RELEASED")
    client.request = original
    client.url = original_url


def test_camunda_invalid_job_does_not_starve_neighbor(distributed, caplog):
    stand = distributed
    service = stand.evaluator.service
    client = service.client
    invalid = client.request("POST", "/process-instances", {
        "processDefinitionKey": client.deploy(),
        "variables": {"assessmentId": "synthetic-sensitive-marker"},
    })
    failures = []
    original_fail = client.fail

    def record_failure(key, retries, delay=1000):
        failures.append((key, retries))
        return original_fail(key, retries, delay)

    client.fail = record_failure
    app = submit(stand)
    until(stand, lambda: status(stand, app) == "APPROVED" and bool(failures), timeout=30)
    assert any(retries == 0 for _, retries in failures)
    assert str(invalid["processInstanceKey"]) != task(stand, app)["process_instance_key"]
    assert len(assessment_sql(stand, "SELECT 1 FROM assessment_task")) == 1
    assert "synthetic-sensitive-marker" not in caplog.text
    assert_single_result(stand, app)


def test_camunda_unexpected_worker_error_does_not_starve_neighbor(distributed):
    stand = distributed
    broken = submit(stand)
    healthy = submit(stand, "other")
    service = stand.evaluator.service
    original = service.checks.call

    def fail_one(stage, customer, consent=None):
        if str(customer) == "10000000-0000-0000-0000-000000000001":
            raise RuntimeError("synthetic-worker-failure")
        return original(stage, customer, consent)

    service.checks.call = fail_one
    until(stand, lambda: stand.request("GET", f"/applications/{healthy}", 200, "other")["status"] == "APPROVED", timeout=30)
    assert status(stand, broken) == "SCORING"
    assert task(stand, broken)["failures"] == 0
    assert not assessment_sql(stand, "SELECT 1 FROM assessment_step WHERE application_id=%s AND stage='PASSPORT'", (broken,))
    service.checks.call = original
    until(stand, lambda: status(stand, broken) == "APPROVED", timeout=30)
    assert_single_result(stand, broken)
    assert_single_result(stand, healthy)


def test_camunda_database_failure_rolls_back_step_and_preserves_neighbor(distributed):
    stand = distributed
    broken = submit(stand)
    wait_for_stage(stand, broken, "PASSPORT")
    healthy = submit(stand, "other")
    assessment_sql(stand, """CREATE FUNCTION training_fail_assessment_write() RETURNS trigger
                            LANGUAGE plpgsql AS $$ BEGIN
                              RAISE EXCEPTION USING ERRCODE='08006', MESSAGE='synthetic-database-outage';
                            END $$""")
    assessment_sql(stand, sql.SQL("""CREATE TRIGGER training_database_outage BEFORE UPDATE
                                  ON assessment_task FOR EACH ROW
                                  WHEN (OLD.application_id={} AND OLD.stage='PASSPORT')
                                  EXECUTE FUNCTION training_fail_assessment_write()""").format(sql.Literal(broken)))
    service = stand.evaluator.service
    failed_keys = []
    original_fail = service.client.fail

    def failed(key, retries, delay=1000):
        failed_keys.append(str(key))
        return original_fail(key, retries, delay)

    service.client.fail = failed
    until(stand, lambda: bool(failed_keys) and
          stand.request("GET", f"/applications/{healthy}", 200, "other")["status"] == "APPROVED", timeout=30)
    assert task(stand, broken)["stage"] == "PASSPORT"
    assert task(stand, broken)["failures"] == 0
    assert not assessment_sql(stand, "SELECT 1 FROM assessment_step WHERE application_id=%s AND stage='PASSPORT'", (broken,))
    assert not assessment_sql(stand, "SELECT 1 FROM assessment_job_receipt WHERE job_key=ANY(%s)", (failed_keys,))
    assert not assessment_sql(stand, "SELECT 1 FROM event_outbox WHERE application_id=%s", (broken,))
    assessment_sql(stand, "DROP TRIGGER training_database_outage ON assessment_task")
    assessment_sql(stand, "DROP FUNCTION training_fail_assessment_write()")
    restart(stand)
    until(stand, lambda: status(stand, broken) == "APPROVED", timeout=30)
    assert assessment_sql(stand, "SELECT attempt FROM assessment_step WHERE application_id=%s AND stage='PASSPORT'", (broken,)) == [
        {"attempt": 1}]
    assert_single_result(stand, broken)
    assert_single_result(stand, healthy)


@pytest.mark.parametrize(("owner", "requested"), [("local", "camunda"), ("camunda", "local")])
def test_assessment_cli_refuses_changing_owner_of_pending_task(distributed, monkeypatch, owner, requested):
    stand = distributed
    app = submit(stand)
    payload, = stand.sql("SELECT payload FROM event_outbox WHERE application_id=%s", (app,))
    with monkeypatch.context() as patch:
        patch.setenv("LOANFLOW_ASSESSMENT_ENGINE", owner)
        with store.connect(stand.assessment_dsn) as db:
            assessment_service.accept_request(db, payload["payload"], stand.clock)
    result = subprocess.run([sys.executable, "-m", "services.loanflow.events", "assessment"],
                            cwd=store.ROOT,
                            env=dict(os.environ, LOANFLOW_DSN=stand.assessment_dsn,
                                     LOANFLOW_ASSESSMENT_ENGINE=requested),
                            text=True, capture_output=True, timeout=10)
    assert result.returncode != 0
    assert "Завершите текущую оценку перед сменой исполнителя" in result.stderr
    assert task(stand, app)["engine"] == owner
    assert task(stand, app)["stage"] == "AML"
    assert not assessment_sql(stand, "SELECT 1 FROM assessment_step")
    assert not assessment_sql(stand, "SELECT 1 FROM event_outbox")


def test_assessment_workers_process_only_tasks_they_own(distributed, monkeypatch):
    stand = distributed
    local_app, camunda_app = submit(stand), submit(stand, "other")
    for app, owner in ((local_app, "local"), (camunda_app, "camunda")):
        payload, = stand.sql("SELECT payload FROM event_outbox WHERE application_id=%s", (app,))
        with monkeypatch.context() as patch:
            patch.setenv("LOANFLOW_ASSESSMENT_ENGINE", owner)
            with store.connect(stand.assessment_dsn) as db:
                assessment_service.accept_request(db, payload["payload"], stand.clock)
    local = assessment_service.AssessmentService(stand.assessment_dsn, stand.clock,
                                                stand.evaluator.service.state, stand.evaluator.service.checks)
    assert local.run_once()
    assert task(stand, local_app)["stage"] == "PASSPORT"
    assert task(stand, camunda_app)["stage"] == "AML"
    wait_for_stage(stand, camunda_app, "PASSPORT")
    assert task(stand, local_app)["stage"] == "PASSPORT"
    assert task(stand, local_app)["process_instance_key"] is None
    assert not assessment_sql(stand, "SELECT 1 FROM assessment_step WHERE application_id=%s", (local_app,))
    until(stand, lambda: stand.request("GET", f"/applications/{camunda_app}", 200, "other")["status"] == "APPROVED", timeout=30)
    while local.run_once():
        pass
    until(stand, lambda: status(stand, local_app) == "APPROVED", timeout=30)
    assert len(stand.sql("SELECT 1 FROM final_decision")) == 2


def test_camunda_separate_processes_signing_and_unknown_abs(distributed, monkeypatch, tmp_path):
    kafka_disbursement(distributed, monkeypatch, tmp_path)


def test_camunda_core_cooling_and_lost_abs_answer_never_resend(distributed, monkeypatch):
    from services.loanflow.abs import post_pending

    stand = distributed
    update_scenario(stand, {"payroll_income_rub": "80000.00"})
    app = submit(stand, amount="20000000")
    until(stand, lambda: status(stand, app) == "APPROVED", timeout=30)
    until(stand, lambda: stand.request("GET", f"/applications/{app}/offer", 200)["state"] == "AVAILABLE")
    offer = stand.request("GET", f"/applications/{app}/offer", 200)
    if offer["pdn_notice_required"]:
        stand.request("PUT", f"/applications/{app}/terms/{offer['terms_id']}/pdn-notice-acknowledgement",
                      200, body={"notice_version": offer["pdn_notice"]["version"]})
    attempt = stand.request("POST", f"/applications/{app}/signing-attempts", 201, key=uuid4())
    path = f"/applications/{app}/signing-attempts/{attempt['attempt_id']}"
    until(stand, lambda: bool(stand.sql("SELECT 1 FROM stand_sms_delivery")))
    stand.request("POST", path + "/verification", 202, key=uuid4(),
                  body={"code": stand_code(attempt["attempt_id"])})
    until(stand, lambda: bool(stand.sql("SELECT 1 FROM contract")))
    contract, = stand.sql("SELECT contract_id FROM contract")
    contract_id = contract["contract_id"]
    view = stand.request("GET", f"/contracts/{contract_id}", 200)
    assert view["status"] == "COOLING_OFF"
    assert datetime.fromisoformat(view["cooling_ends_at"]) - stand.clock() == timedelta(hours=4)
    assert not stand.sql("SELECT 1 FROM disbursement_outbox")
    with abs_server(stand, monkeypatch) as client:
        original_call = client.call

        def discard_send_answer(kind, *args):
            response = original_call(kind, *args)
            return None if kind == "SEND" else response

        client.call = discard_send_answer
        stand.core.service.abs = client
        stand.clock.at = datetime.fromisoformat(view["cooling_ends_at"])
        until(stand, lambda: bool(stand.sql("SELECT 1 FROM training_abs.instruction")))
        assert stand.request("GET", f"/contracts/{contract_id}", 200)["status"] == "DISBURSEMENT_PROCESSING"
        instruction, = stand.sql("SELECT instruction_id FROM disbursement_instruction")
        instruction_id = instruction["instruction_id"]
        stand.core.close()
        stand.core = EventProcess("core", stand.dsn, stand.clock, group="core-" + stand.group)
        stand.core.service.abs = client
        for _ in range(10):
            stand.core.run_once()
            stand.evaluator.run_once()
        assert stand.sql("SELECT count(*) AS n FROM training_abs.call WHERE operation='SEND'") == [{"n": 1}]
        assert_reserve(stand, "HELD")
        with store.connect(stand.dsn) as db:
            post_pending(db, instruction_id)
        stand.request("POST", f"/reconciliation/instructions/{instruction_id}/reads", 202,
                      "reconciliation", key=uuid4())
        until(stand, lambda: stand.request("GET", f"/contracts/{contract_id}", 200)["status"] == "DISBURSED")
        assert stand.sql("SELECT count(*) AS n FROM training_abs.call WHERE operation='SEND'") == [{"n": 1}]
        assert_reserve(stand, "CONSUMED")
