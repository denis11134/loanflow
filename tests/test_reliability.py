"""Глава 4.4: восстановление сохранённых команд и границы сетевых сбоев."""
from datetime import timedelta
from http.client import IncompleteRead
from uuid import uuid4

import pytest

from services.loanflow import http, reconciliation, signing
from services.loanflow.abs import post_pending
from services.loanflow.checks import CHECK_STAGES, ChecksClient
from services.loanflow.store import as_json, connect
from test_service import (prepare_offer, process, setup_stand, signed_contract, start_attempt,
                          submit, verify, worker)
from test_service_abs import (abs_control, abs_server, calls, instruction_id, read, register)

pytestmark = pytest.mark.service


@pytest.mark.parametrize("kind", CHECK_STAGES)
@pytest.mark.parametrize("error", [TimeoutError, ConnectionResetError, IncompleteRead])
def test_check_transport_failure_preserves_retry_schedule(setup_stand, monkeypatch, kind, error):
    stand = setup_stand
    client = ChecksClient()
    timeouts = []

    def unavailable(request, timeout):
        timeouts.append(timeout)
        raise error(b"incomplete")

    monkeypatch.setattr(client.opener, "open", unavailable)
    with stand.serve():
        app = submit(stand)
        stages = ("AML", *CHECK_STAGES)
        process(stand, *stages[:stages.index(kind)])
        for count, delay in enumerate((1, 3), start=1):
            process(stand, kind, checks_client=client)
            due = stand.clock() + timedelta(seconds=delay)
            assert stand.sql("SELECT failures,run_at FROM service_job WHERE kind=%s", (kind,)) == [
                {"failures": count, "run_at": due}]
            stand.clock.at = due - timedelta(microseconds=1)
            assert not worker(stand, checks_client=client).run_once(kind)
            stand.clock.at = due
        process(stand, kind, checks_client=client)
        assert timeouts == [2, 2, 2]
        assert stand.request("GET", f"/applications/{app}", 200)["status"] == "SYSTEM_ERROR"
        assert not stand.sql("SELECT 1 FROM service_job")
        assert [row["operation"] for row in stand.sql(
            "SELECT operation FROM stand_adapter_call ORDER BY call_id"
        )] == [*stages[:stages.index(kind)], kind, kind, kind]


@pytest.mark.parametrize("command", ["submission", "sms"])
def test_accepted_command_replays_after_lost_response_and_restart(setup_stand, monkeypatch, command):
    stand = setup_stand
    key = uuid4()
    with stand.serve():
        if command == "sms":
            app, offer = prepare_offer(stand)
            attempt_path, attempt = start_attempt(stand, app, offer)
            path = attempt_path + "/sms-resends"
        else:
            app = stand.draft()
            path = f"/applications/{app}/submission"

        saved = []
        invoke = http.run_in_threadpool

        async def lose_committed_response(*args, **kwargs):
            saved.append(await invoke(*args, **kwargs))
            raise ConnectionResetError("synthetic response loss after commit")

        with monkeypatch.context() as patch:
            patch.setattr(http, "run_in_threadpool", lose_committed_response)
            assert stand.request("POST", path, 503, key=key)["code"] == "SERVICE_UNAVAILABLE"
        assert len(saved) == 1 and saved[0][0] == 202
        if command == "sms":
            process(stand, "SMS")
            verify(stand, attempt_path, attempt)
            process(stand, "VERIFY")
        else:
            process(stand, "AML", "PASSPORT", "CREDIT_REPORT", "FRAUD", "DE", "PUBLISH")

    with stand.serve():
        for _ in range(2):
            assert stand.request("POST", path, 202, key=key) == as_json(saved[0][1])
        if command == "sms":
            assert stand.sql("SELECT sms_resend_count FROM signing_attempt") == [{"sms_resend_count": 1}]
            assert stand.sql("SELECT count(*) AS n FROM stand_sms_delivery") == [{"n": 2}]
            assert not stand.sql("SELECT 1 FROM service_job WHERE kind='SMS'")
            assert stand.request("POST", path, 409, key=uuid4())["code"] == "SIGNING_ATTEMPT_CLOSED"
        else:
            assert stand.sql("SELECT count(*) AS n FROM evaluation") == [{"n": 1}]
            assert stand.sql("SELECT count(*) AS n FROM submission_snapshot") == [{"n": 1}]
            assert not stand.sql("SELECT 1 FROM service_job WHERE kind='AML'")
            assert stand.request("GET", f"/applications/{app}", 200)["status"] == "APPROVED"


def test_sms_failure_before_commit_rolls_back_command_and_effect(setup_stand, monkeypatch):
    stand = setup_stand
    with stand.serve():
        app, offer = prepare_offer(stand)
        attempt_path, _ = start_attempt(stand, app, offer)
    original = signing.resend_signing_sms

    def fail_after_effect(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic failure before command commit")

    key = uuid4()
    with monkeypatch.context() as patch:
        patch.setattr(signing, "resend_signing_sms", fail_after_effect)
        with stand.serve():
            stand.request("POST", attempt_path + "/sms-resends", 503, key=key)
    assert stand.sql("SELECT sms_resend_count FROM signing_attempt") == [{"sms_resend_count": 0}]
    assert not stand.sql("SELECT 1 FROM accepted_request WHERE request_key=%s", (key,))
    assert not stand.sql("SELECT 1 FROM service_job WHERE kind='SMS'")
    with stand.serve():
        receipt = stand.request("POST", attempt_path + "/sms-resends", 202, key=key)
        assert receipt["resend_number"] == 1
        assert stand.request("POST", attempt_path + "/sms-resends", 202, key=key) == receipt
        process(stand, "SMS")
        assert stand.sql("SELECT count(*) AS n FROM stand_sms_delivery") == [{"n": 2}]


def test_abs_truncated_read_finishes_as_timeout_without_retry(setup_stand, monkeypatch):
    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        process(stand, "SEND", abs_client=client)
        path = f"/reconciliation/instructions/{instruction}"
        register(stand, path)
        timeouts = []

        class TruncatedResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def read(self):
                raise IncompleteRead(b'{"instruction_id":')

        def truncated(request, timeout):
            timeouts.append(timeout)
            return TruncatedResponse()

        with monkeypatch.context() as patch:
            patch.setattr(client.opener, "open", truncated)
            process(stand, "ABS_READ", abs_client=client)
        assert timeouts == [2]
        assert read(stand, path, 1) == "TIMEOUT"
        assert not stand.sql("SELECT 1 FROM service_job WHERE kind='ABS_READ'")
        stand.clock.at += timedelta(seconds=1)
        assert not worker(stand, abs_client=client).run_once("ABS_READ")
        assert stand.request("PUT", path + "/closure", 409, "reconciliation")["code"] == "CLOSURE_NOT_AUTHORIZED_BY_READ"
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        assert read(stand, path, 2) == "PENDING"


@pytest.mark.parametrize(("behavior", "terminal"), [
    ("UNAVAILABLE", "CLOSED"), ("DROP_RESPONSE", "CLOSED"), ("DROP_RESPONSE", "POSTED"),
])
def test_unknown_send_recovers_only_by_reconciliation(setup_stand, monkeypatch, behavior, terminal):
    stand = setup_stand
    with stand.serve(), abs_server(stand, monkeypatch) as client:
        contract = signed_contract(stand, "49999.00")
        instruction = instruction_id(stand)
        abs_control(stand, instruction, "SEND", behavior)
        process(stand, "SEND", abs_client=client)
        assert stand.request("GET", f"/contracts/{contract}", 200)["status"] == "DISBURSEMENT_PROCESSING"
        assert stand.sql("SELECT state FROM mpl_reservation") == [{"state": "HELD"}]
        assert stand.sql("SELECT state FROM disbursement_outbox") == [{"state": "STARTED"}]
        stand.clock.at += timedelta(seconds=30)
        assert not worker(stand, abs_client=client).run_once("SEND")
        assert calls(stand, instruction, "SEND") == 1
        path = f"/reconciliation/instructions/{instruction}"
        assert [r["instruction_id"] for r in stand.request(
            "GET", "/reconciliation/instructions", 200, "reconciliation"
        )] == [str(instruction)]
        register(stand, path)
        process(stand, "ABS_READ", abs_client=client)
        assert read(stand, path, 1) == ("NOT_FOUND" if behavior == "UNAVAILABLE" else "PENDING")

        if terminal == "POSTED":
            # После потери ответа распоряжение уже принято АБС; подтверждаем проводку чтением.
            with connect(stand.dsn) as db:
                post_pending(db, instruction, stand.clock)
            register(stand, path)
            process(stand, "ABS_READ", abs_client=client)
        if terminal == "CLOSED":
            stand.request("PUT", path + "/closure", 202, "reconciliation")
            process(stand, "ABS_CLOSE", abs_client=client)

        result = client.call("READ", instruction, 2)
        assert result[1]["state"] == terminal
        tables = ("mpl_reservation", "abs_final_result", "contract", "disbursement_outbox")
        before = [stand.sql(f"SELECT * FROM {table}") for table in tables]
        stand.clock.at += timedelta(seconds=1)
        # Поздние дубли результата не меняют даже время перехода резерва.
        for operation in ("SEND", "CLOSE"):
            with connect(stand.dsn) as db:
                reconciliation.handle_response(db, instruction, operation, result, stand.clock)
        assert [stand.sql(f"SELECT * FROM {table}") for table in tables] == before
        final = stand.request("PUT", path + "/closure", 200, "reconciliation")
        assert final["final_result"] == terminal
        assert final["contract_status"] == ("DISBURSED" if terminal == "POSTED" else "DISBURSEMENT_FAILED")
        assert stand.sql("SELECT state FROM mpl_reservation") == [
            {"state": "CONSUMED" if terminal == "POSTED" else "RELEASED"}]
        assert calls(stand, instruction, "SEND") == 1
