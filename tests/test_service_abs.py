from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from uuid import uuid4
import threading

import pytest

from services.loanflow import assessment, reconciliation
from services.loanflow.abs import create_abs, post_pending
from services.loanflow.store import connect, enqueue, migrate
from services.loanflow.worker import AbsClient
from test_service import (SimulatedProcessStop, assert_reserve, crash_at, http_server, pause_at, process, setup_stand,
                          signed_contract, submit, update_scenario, worker)

pytestmark = pytest.mark.service


@contextmanager
def abs_server(stand, monkeypatch, checkpoint=lambda *_: None):
    monkeypatch.setenv("LOANFLOW_ABS_TOKEN", "synthetic-abs-test-token")
    with http_server(create_abs(stand.dsn, stand.clock, checkpoint)) as port:
        yield AbsClient(f"http://127.0.0.1:{port}")


def instruction_id(stand):
    row, = stand.sql("SELECT instruction_id FROM disbursement_instruction")
    return row["instruction_id"]


def abs_control(stand, instruction, operation, behavior):
    stand.sql("""INSERT INTO training_abs.control VALUES (%s,%s,%s)
                 ON CONFLICT (instruction_id,operation) DO UPDATE SET behavior=excluded.behavior""",
              (instruction, operation, behavior))


def calls(stand, instruction, operation):
    return stand.sql("SELECT count(*) AS n FROM training_abs.call WHERE instruction_id=%s AND operation=%s",
                     (instruction, operation))[0]["n"]


def register(stand, path, key=None):
    return stand.request("POST", path + "/reads", 202, "reconciliation", key=key or uuid4())


def read(stand, path, sequence):
    return stand.request("GET", f"{path}/reads/{sequence}", 200, "reconciliation")["result"]


def test_failed_job_does_not_block_application_timer_or_send(setup_stand, monkeypatch):
    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        contract = signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        abs_control(stand, instruction, "SEND", "AUTO_POST")
        broken = uuid4()
        with connect(stand.dsn) as db:
            enqueue(db, "AML", broken, stand.clock() - timedelta(seconds=1))
        update_scenario(stand, {"aml_block": True}, "other")
        healthy = submit(stand, "other")

        instance = worker(stand, abs_client=client)
        run_job = instance.run_job

        def fail_first(db, job):
            if job["resource_id"] == broken:
                assessment.audit(db, job["kind"], broken, stand.clock())
                db.execute("SELECT 1 / 0")
            return run_job(db, job)

        monkeypatch.setattr(instance, "run_job", fail_first)
        assert instance.run_once()
        assert stand.request("GET", f"/applications/{healthy}", 200, "other")["status"] == "BANK_REJECTED"
        assert not stand.sql("SELECT 1 FROM stand_adapter_call WHERE resource_id=%s", (broken,))
        delayed, = stand.sql("SELECT failures,run_at FROM service_job WHERE resource_id=%s", (broken,))
        assert delayed["failures"] == 0 and delayed["run_at"] > stand.clock()

        stand.clock.at += timedelta(minutes=5)
        assert stand.sql("SELECT 1 FROM service_job WHERE kind='ATTEMPT_EXPIRY'")
        assert instance.run_once()
        assert not stand.sql("SELECT 1 FROM service_job WHERE kind='ATTEMPT_EXPIRY'")
        assert not stand.sql("SELECT 1 FROM stand_adapter_call WHERE resource_id=%s", (broken,))

        assert instance.run_once()
        assert calls(stand, instruction, "SEND") == 1
        assert stand.request("GET", f"/contracts/{contract}", 200)["status"] == "DISBURSED"


@pytest.mark.parametrize(("checkpoint", "expected_calls", "state"), [
    ("before_started_commit", 1, "PENDING"),
    ("after_started_commit", 0, "NOT_FOUND"),
    ("after_send", 1, "PENDING"),
])
def test_outbox_crash_boundaries(setup_stand, monkeypatch, checkpoint, expected_calls, state):
    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        with pytest.raises(SimulatedProcessStop):
            worker(stand, abs_client=client, checkpoint=crash_at(checkpoint)).run_once("SEND")
        restarted = worker(stand, abs_client=client)
        assert restarted.run_once("SEND") == (checkpoint == "before_started_commit")
        assert not restarted.run_once("SEND")
        assert calls(stand, instruction, "SEND") == expected_calls
        queue = stand.request("GET", "/reconciliation/instructions", 200, "reconciliation")
        assert [r["instruction_id"] for r in queue] == [str(instruction)]
        path = f"/reconciliation/instructions/{instruction}"
        first = register(stand, path)
        assert restarted.run_once("ABS_READ")
        assert read(stand, path, first["read_seq"]) == state
        assert_reserve(stand, "HELD")


@pytest.mark.parametrize("first_worker", [0, 1])
def test_two_workers_send_once_and_post(setup_stand, monkeypatch, first_worker):
    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        contract = signed_contract(stand, "49999.99")
        instruction = instruction_id(stand)
        abs_control(stand, instruction, "SEND", "AUTO_POST")
        entered, proceed = threading.Event(), threading.Event()
        workers = [worker(stand, abs_client=client) for _ in range(2)]
        workers[first_worker].checkpoint = pause_at("before_started_commit", entered, proceed)
        with ThreadPoolExecutor(max_workers=2) as pool:
            result = pool.submit(workers[first_worker].run_once, "SEND")
            assert entered.wait(5)
            assert not workers[1 - first_worker].run_once("SEND")
            proceed.set()
            assert result.result(5)
        assert calls(stand, instruction, "SEND") == 1
        assert stand.request("GET", f"/contracts/{contract}", 200)["status"] == "DISBURSED"
        path = f"/reconciliation/instructions/{instruction}"
        assert stand.request("PUT", path + "/closure", 200, "reconciliation")["final_result"] == "POSTED"
        assert stand.request("GET", path, 200, "reconciliation")["can_close"] is False
        assert calls(stand, instruction, "CLOSE") == 0
        assert_reserve(stand, "CONSUMED")


def test_close_before_send_and_restore_original_read(setup_stand, monkeypatch):
    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        contract = signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        with pytest.raises(SimulatedProcessStop):
            worker(stand, abs_client=client, checkpoint=crash_at("after_started_commit")).run_once("SEND")
        path = f"/reconciliation/instructions/{instruction}"
        key = uuid4()
        receipt = register(stand, path, key)
        with pytest.raises(SimulatedProcessStop):
            worker(stand, abs_client=client, checkpoint=crash_at("after_read")).run_once("ABS_READ")
        assert read(stand, path, 1) == "WAITING"
        assert register(stand, path, key) == receipt
        stand.clock.at += timedelta(seconds=9)
        process(stand, "ABS_READ", abs_client=client)
        assert calls(stand, instruction, "READ") == 2
        assert read(stand, path, 1) == "NOT_FOUND"
        assert stand.request("PUT", path + "/closure", 202, "reconciliation")["close_command_accepted"]
        process(stand, "ABS_CLOSE", abs_client=client)
        final = stand.request("PUT", path + "/closure", 200, "reconciliation")
        assert final["final_result"] == "CLOSED"
        assert stand.request("GET", f"/contracts/{contract}", 200)["status"] == "DISBURSEMENT_FAILED"
        assert_reserve(stand, "RELEASED")
        status, body, valid = client.call("SEND", instruction, 2,
                                          {"account_ref": "stand-account-1", "amount_minor": 4999900, "currency": "RUB"})
        assert (status, body["state"], valid) == (200, "CLOSED", True)
        assert calls(stand, instruction, "CLOSE") == 1


@pytest.mark.parametrize("post_first", [False, True])
def test_abs_close_and_post_order(setup_stand, monkeypatch, post_first):
    stand = setup_stand
    close_locked, proceed = threading.Event(), threading.Event()

    def checkpoint(name, _id, kind):
        if name == "before_abs_commit" and kind == "CLOSE" and not post_first:
            close_locked.set()
            assert proceed.wait(5)

    with stand.serve(), abs_server(stand, monkeypatch, checkpoint) as client:
        signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        path = f"/reconciliation/instructions/{instruction}"
        process(stand, "SEND", abs_client=client)
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        assert read(stand, path, 1) == "PENDING"
        stand.request("PUT", path + "/closure", 202, "reconciliation")

        def post(pause=False):
            with connect(stand.dsn) as db:
                row = post_pending(db, instruction, stand.clock)
                if pause:
                    close_locked.set()
                    assert proceed.wait(5)
                return row["state"]

        with ThreadPoolExecutor(max_workers=2) as pool:
            close = worker(stand, abs_client=client)
            if post_first:
                posting = pool.submit(post, True)
                assert close_locked.wait(5)
                closure = pool.submit(close.run_once, "ABS_CLOSE")
            else:
                closure = pool.submit(close.run_once, "ABS_CLOSE")
                assert close_locked.wait(5)
                posting = pool.submit(post)
            proceed.set()
            expected = "POSTED" if post_first else "CLOSED"
            assert posting.result(5) == expected and closure.result(5)
        assert stand.request("PUT", path + "/closure", 200, "reconciliation")["final_result"] == expected
        assert calls(stand, instruction, "CLOSE") == 1
        assert_reserve(stand, "CONSUMED" if post_first else "RELEASED")


def test_abs_read_order_timeout_and_close_response_loss(setup_stand, monkeypatch):
    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        path = f"/reconciliation/instructions/{instruction}"
        process(stand, "SEND", abs_client=client)
        assert register(stand, path)["read_seq"] == 1
        entered, proceed = threading.Event(), threading.Event()
        delayed = worker(stand, abs_client=client, checkpoint=pause_at("after_read", entered, proceed))
        with ThreadPoolExecutor() as pool:
            pending = pool.submit(delayed.run_once, "ABS_READ")
            assert entered.wait(5)
            assert register(stand, path)["read_seq"] == 2
            proceed.set()
            assert pending.result(5)
        assert read(stand, path, 1) == "PENDING" and read(stand, path, 2) == "WAITING"
        assert stand.request("PUT", path + "/closure", 409, "reconciliation")["code"] == "CLOSURE_NOT_AUTHORIZED_BY_READ"
        stand.clock.at += timedelta(seconds=10)
        process(stand, "ABS_READ", abs_client=client)
        assert read(stand, path, 2) == "TIMEOUT"
        assert calls(stand, instruction, "READ") == 1
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        stand.request("PUT", path + "/closure", 202, "reconciliation")
        abs_control(stand, instruction, "CLOSE", "DROP_RESPONSE")
        process(stand, "ABS_CLOSE", abs_client=client)
        assert_reserve(stand, "HELD")
        register(stand, path)
        assert read(stand, path, 4) == "WAITING"
        stand.request("PUT", path + "/closure", 202, "reconciliation")
        stand.sql("DELETE FROM training_abs.control")
        process(stand, "ABS_READ", abs_client=client)
        process(stand, "ABS_CLOSE", abs_client=client)
        assert read(stand, path, 4) == "CLOSED"
        assert stand.request("PUT", path + "/closure", 200, "reconciliation")["final_result"] == "CLOSED"
        assert calls(stand, instruction, "CLOSE") == 1
        assert_reserve(stand, "RELEASED")


@pytest.mark.parametrize(("field", "value", "category"), [
    ("instruction_id", str(uuid4()), "INSTRUCTION_ID"),
    ("account_ref", "stand-sensitive-account", "ACCOUNT"),
    ("amount_minor", 17, "AMOUNT"),
    ("currency", "USD", "CURRENCY"),
])
def test_posted_wrong_correlation_does_not_change_reserve(setup_stand, monkeypatch, field, value, category):
    from services.loanflow import abs

    stand = setup_stand
    original = abs.payload

    def corrupt(instruction, row):
        result = original(instruction, row)
        if result["state"] == "POSTED":
            result[field] = value
        return result

    with stand.serve(), abs_server(stand, monkeypatch) as client:
        contract = signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        abs_control(stand, instruction, "SEND", "AUTO_POST")
        monkeypatch.setattr(abs, "payload", corrupt)
        process(stand, "SEND", abs_client=client)
        assert stand.request("GET", f"/contracts/{contract}", 200)["status"] == "DISBURSEMENT_PROCESSING"
        path = f"/reconciliation/instructions/{instruction}"
        card = stand.request("GET", path, 200, "reconciliation")
        diagnostic = card["last_correlation_error"]
        assert set(diagnostic) == {"operation", "mismatch_kind", "observed_at"}
        assert diagnostic["mismatch_kind"] == category
        assert stand.sql("SELECT instruction_id,operation,mismatch_kind FROM abs_correlation_error") == [{
            "instruction_id": instruction, "operation": "SEND", "mismatch_kind": category,
        }]
        assert_reserve(stand, "HELD")
        assert not stand.sql("SELECT * FROM abs_final_result")
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        assert read(stand, path, 1) == "CORRELATION_ERROR"
        assert stand.sql("SELECT response_instruction_id FROM abs_read WHERE read_seq=1") == [
            {"response_instruction_id": None},
        ]
        monkeypatch.setattr(abs, "payload", original)
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        assert read(stand, path, 2) == "POSTED"
        assert_reserve(stand, "CONSUMED")
        if field == "instruction_id":
            # Состояние базы до миграции 009.
            stand.sql("DELETE FROM service_migration WHERE name='009_abs_read_diagnostics.sql'")
            stand.sql("UPDATE abs_read SET response_instruction_id=%s WHERE read_seq=1", (value,))
            migrate(stand.dsn)
            assert stand.sql("SELECT result,response_instruction_id FROM abs_read ORDER BY read_seq") == [
                {"result": "CORRELATION_ERROR", "response_instruction_id": None},
                {"result": "POSTED", "response_instruction_id": instruction},
            ]


def test_final_apply_rollback_recovers_by_read(setup_stand, monkeypatch):
    stand = setup_stand

    def fail_before_commit(name, _):
        if name == "before_abs_result_commit":
            raise RuntimeError("failed to apply ABS result")

    with stand.serve(), abs_server(stand, monkeypatch) as client:
        contract = signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        abs_control(stand, instruction, "SEND", "AUTO_POST")
        instance = process(stand, "SEND", abs_client=client, checkpoint=fail_before_commit)
        assert not instance.run_once("SEND")
        assert stand.request("GET", f"/contracts/{contract}", 200)["status"] == "DISBURSEMENT_PROCESSING"
        assert_reserve(stand, "HELD")
        assert stand.sql("SELECT state FROM disbursement_outbox") == [{"state": "STARTED"}]
        assert not stand.sql("SELECT * FROM abs_final_result")
        queue = stand.request("GET", "/reconciliation/instructions", 200, "reconciliation")
        assert [row["instruction_id"] for row in queue] == [str(instruction)]
        path = f"/reconciliation/instructions/{instruction}"
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        assert stand.request("GET", f"/contracts/{contract}", 200)["status"] == "DISBURSED"
        assert_reserve(stand, "CONSUMED")
        assert calls(stand, instruction, "SEND") == 1


def test_old_terminal_read_after_new_conflict(setup_stand, monkeypatch):
    stand = setup_stand
    entered, proceed = threading.Event(), threading.Event()
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        path = f"/reconciliation/instructions/{instruction}"
        process(stand, "SEND", abs_client=client)
        register(stand, path)
        abs_control(stand, instruction, "READ", "READ_CONFLICT")
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(worker(stand, abs_client=client, checkpoint=pause_at("before_abs_read", entered, proceed))
                                .run_once, "ABS_READ")
            assert entered.wait(5)
            assert register(stand, path)["read_seq"] == 2
            process(stand, "ABS_READ", abs_client=client)
            assert read(stand, path, 2) == "CONFLICT"
            assert stand.request("PUT", path + "/closure", 409, "reconciliation")["code"] == "CLOSURE_NOT_AUTHORIZED_BY_READ"
            with connect(stand.dsn) as db:
                assert post_pending(db, instruction, stand.clock)["state"] == "POSTED"
            proceed.set()
            assert first.result(5)
        assert read(stand, path, 1) == "POSTED"
        card = stand.request("GET", path, 200, "reconciliation")
        assert card["last_read"]["result"] == "CONFLICT" and card["final_result"] == "POSTED"
        assert_reserve(stand, "CONSUMED")


@pytest.mark.parametrize("single", [True, False], ids=["card", "queue"])
def test_reconciliation_read_during_terminal_transition(setup_stand, monkeypatch, single):
    stand = setup_stand
    entered, proceed = threading.Event(), threading.Event()
    pause = pause_at("reconciliation_read", entered, proceed)
    original = reconciliation.view

    def paused_view(db, row):
        if row["contract_status"] == "DISBURSEMENT_PROCESSING":
            pause("reconciliation_read")
        return original(db, row)

    with stand.serve(), abs_server(stand, monkeypatch) as client:
        signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        queue = "/reconciliation/instructions"
        path = f"{queue}/{instruction}"
        process(stand, "SEND", abs_client=client)
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        read_path = path if single else queue
        before = stand.request("GET", read_path, 200, "reconciliation")
        processing = before if single else before[0]
        assert processing["last_read"]["result"] == "PENDING"
        assert processing["can_close"]

        monkeypatch.setattr(reconciliation, "view", paused_view)
        with ThreadPoolExecutor() as pool:
            reading = pool.submit(stand.request, "GET", read_path, 200, "reconciliation")
            try:
                assert entered.wait(5)
                with connect(stand.dsn) as db:
                    assert post_pending(db, instruction, stand.clock)["state"] == "POSTED"
                register(stand, path)
                process(stand, "ABS_READ", abs_client=client)
            finally:
                proceed.set()
            assert reading.result(5) == before

        after = stand.request("GET", path, 200, "reconciliation")
        assert after["contract_status"] == "DISBURSED" and after["final_result"] == "POSTED"
        assert after["last_read"]["result"] == "POSTED" and not after["can_close"]
        assert stand.request("GET", queue, 200, "reconciliation") == []


def test_close_job_recovers_before_first_call_and_read_times_out(setup_stand, monkeypatch):
    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        path = f"/reconciliation/instructions/{instruction}"
        process(stand, "SEND", abs_client=client)
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        stand.request("PUT", path + "/closure", 202, "reconciliation")
        with pytest.raises(SimulatedProcessStop):
            worker(stand, abs_client=client, checkpoint=crash_at("before_job")).run_once("ABS_CLOSE")
        assert calls(stand, instruction, "CLOSE") == 0
        assert len(stand.sql("SELECT * FROM abs_close_command")) == 1
        abs_control(stand, instruction, "READ", "DROP_RESPONSE")
        register(stand, path)
        stand.clock.at += timedelta(seconds=9, milliseconds=500)
        original = client.call
        timeouts = []

        def recorded_call(kind, instruction, timeout, body=None):
            timeouts.append(timeout)
            return original(kind, instruction, timeout, body)

        monkeypatch.setattr(client, "call", recorded_call)
        process(stand, "ABS_READ", abs_client=client)
        assert timeouts == [0.5]
        assert read(stand, path, 2) == "TIMEOUT"
        assert stand.request("GET", path, 200, "reconciliation")["can_retry_close"] is True
        process(stand, "ABS_CLOSE", abs_client=client)
        assert stand.request("PUT", path + "/closure", 200, "reconciliation")["final_result"] == "CLOSED"
        assert calls(stand, instruction, "CLOSE") == 1
        assert_reserve(stand, "RELEASED")
