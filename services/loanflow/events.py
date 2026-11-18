"""Точки запуска обмена и отдельного сервиса оценки."""
import argparse
import logging
import os
import signal
from threading import Event
from time import sleep

from . import assessment_service, core_events, event_bus, store
from .worker import Worker, no_checkpoint

log = logging.getLogger("loanflow.events")


class EventProcess:
    def __init__(self, role, dsn=None, clock=store.now, checkpoint=no_checkpoint,
                 state_client=None, checks_client=None, bootstrap=None, group=None):
        self.role = role
        self.dsn = dsn or os.environ["LOANFLOW_DSN"]
        self.clock = clock
        self.checkpoint = checkpoint
        self.topic = event_bus.RESULT_TOPIC if role == "core" else event_bus.REQUEST_TOPIC
        self.bus = event_bus.KafkaBus(self.topic, group or f"loanflow-{role}-v1", bootstrap)
        self.service = (Worker(self.dsn, clock) if role == "core" else
                        assessment_service.AssessmentService(self.dsn, clock, state_client, checks_client, checkpoint))

    def handler(self, db, payload):
        if self.role == "core":
            return core_events.apply_result(db, payload, self.clock)
        return assessment_service.accept_request(db, payload, self.clock)

    def run_once(self):
        operations = (
            lambda: self.bus.consume_one(self.dsn, self.topic, self.handler, self.clock, self.checkpoint),
            lambda: self.bus.publish_one(self.dsn, self.clock, self.checkpoint),
            self.service.run_once,
        )
        worked = False
        for operation in operations:
            try:
                worked = operation() or worked
            except Exception as exc:
                log.error("event_operation_failed role=%s category=%s", self.role, type(exc).__name__)
        return worked

    def close(self):
        self.bus.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("core", "assessment", "migrate", "topics"))
    args = parser.parse_args()
    if args.action == "topics":
        event_bus.create_topics()
        return
    if args.action == "migrate":
        assessment_service.migrate(os.environ["LOANFLOW_DSN"])
        return
    if args.action == "core":
        with store.connect() as db:
            unfinished = store.one(db, """SELECT 1 FROM service_job j JOIN application a
                                         ON a.application_id=j.resource_id
                                         WHERE a.status='SCORING'
                                         AND j.kind IN ('AML','PASSPORT','CREDIT_REPORT','FRAUD','DE') LIMIT 1""")
            if unfinished:
                raise SystemExit("Завершите старую оценку перед включением Kafka")
    process = EventProcess(args.action)
    stopping = Event()
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    signal.signal(signal.SIGINT, lambda *_: stopping.set())
    try:
        while not stopping.is_set():
            try:
                if process.run_once():
                    continue
            except Exception as exc:
                log.error("event_process_failed role=%s category=%s", args.action, type(exc).__name__)
            sleep(0.1)
    finally:
        process.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"Процесс обмена завершился ошибкой ({type(exc).__name__})") from None
