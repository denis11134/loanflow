"""Ожидание инфраструктуры терпит временный отказ, но не скрывает ошибку проверки."""
import pytest
from confluent_kafka import KafkaError, KafkaException

from nfr_support import until
from nfr_support import Runtime
from test_database import temporary_postgres
import psycopg


def test_broker_transport_failure_is_retried_during_startup():
    calls = []
    def metadata():
        calls.append(True)
        if len(calls) == 1:
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        return True
    assert until(metadata, 1, "Не готов брокер")
    assert len(calls) == 2


def test_failed_acceptance_assertion_is_not_retried():
    def failed():
        raise AssertionError("Нарушен проверяемый инвариант")
    with pytest.raises(AssertionError, match="инвариант"):
        until(failed, 1, "Не готов брокер")


def test_database_crash_preserves_socket_logging_and_committed_data(tmp_path):
    with temporary_postgres() as (params, command):
        runtime = Runtime(tmp_path)
        runtime.database_controls["core"] = (params, command)
        runtime.ready = lambda: True
        with psycopg.connect(**params) as db:
            db.execute("CREATE TABLE nfr_marker (value integer)")
            db.execute("INSERT INTO nfr_marker VALUES (1)")
        runtime.crash("core_database")
        runtime.restore("core_database")
        with psycopg.connect(**params) as db:
            assert db.execute("SELECT value FROM nfr_marker").fetchall() == [(1,)]
            assert db.execute("SHOW listen_addresses").fetchone() == ("",)
            assert db.execute("SHOW unix_socket_directories").fetchone() == (params["host"],)
            assert db.execute("SHOW log_min_error_statement").fetchone() == ("panic",)
