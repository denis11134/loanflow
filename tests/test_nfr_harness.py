"""Ожидание инфраструктуры терпит временный отказ, но не скрывает ошибку проверки."""
import pytest
from confluent_kafka import KafkaError, KafkaException

from nfr_support import until


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
