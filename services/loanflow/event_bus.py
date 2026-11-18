"""Kafka, транзакционные outbox/inbox и безопасная проверка AsyncAPI."""
import json
import os
from datetime import datetime
from functools import lru_cache
from uuid import UUID

import yaml
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from jsonschema import Draft7Validator
from psycopg.types.json import Jsonb

from . import store
from .http import expand

REQUEST_TOPIC = "loanflow.assessment.requests.v1"
RESULT_TOPIC = "loanflow.assessment.results.v1"


def enabled():
    return bool(os.environ.get("LOANFLOW_KAFKA_BOOTSTRAP"))


@lru_cache(maxsize=2)
def validator(topic):
    doc = yaml.safe_load((store.ROOT / "api/assessment.asyncapi.yaml").read_text())
    name = "AssessmentRequested" if topic == REQUEST_TOPIC else "AssessmentCompleted"
    return Draft7Validator(expand(doc, doc["components"]["messages"][name]["payload"]),
                           format_checker=Draft7Validator.FORMAT_CHECKER)


def migrate(db):
    db.execute((store.ROOT / "services/loanflow/event_store.sql").read_text())


def queue(db, topic, payload, at):
    validator(topic).validate(payload)
    db.execute("""INSERT INTO event_outbox (message_id,application_id,topic,payload,created_at)
                  VALUES (%s,%s,%s,%s,%s)""",
               (payload["message_id"], payload["application_id"], topic, Jsonb(payload), at))


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def decode(record, topic):
    try:
        payload = json.loads(record.value())
        if not validator(topic).is_valid(payload):
            return None
        if record.key() != payload["application_id"].encode():
            return None
        timestamp(payload["submitted_at"])
        UUID(payload["message_id"])
        return payload
    except (ValueError, TypeError, RecursionError, UnicodeError):
        return None


def reject(db, record, category, at):
    db.execute("""INSERT INTO event_rejection VALUES (%s,%s,%s,%s,%s)
                  ON CONFLICT DO NOTHING""",
               (record.topic(), record.partition(), record.offset(), category, at))


def receive(db, record, topic, handler, clock):
    payload = decode(record, topic)
    if payload is None:
        reject(db, record, "INVALID_MESSAGE", clock())
        return
    message_id = payload["message_id"]
    fingerprint = store.digest("event", payload)
    db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 533))", (message_id,))
    existing = store.one(db, "SELECT fingerprint FROM event_inbox WHERE message_id=%s", (message_id,))
    if existing:
        if bytes(existing["fingerprint"]) != fingerprint:
            reject(db, record, "MESSAGE_ID_CONFLICT", clock())
        return
    category = handler(db, payload)
    if category:
        reject(db, record, category, clock())
    db.execute("INSERT INTO event_inbox VALUES (%s,%s,%s)", (message_id, fingerprint, clock()))


class KafkaBus:
    def __init__(self, topic, group, bootstrap=None):
        config = {"bootstrap.servers": bootstrap or os.environ["LOANFLOW_KAFKA_BOOTSTRAP"],
                  "log_level": 0}
        self.producer = Producer({**config, "enable.idempotence": True, "acks": "all",
                                  "message.timeout.ms": 5000})
        self.consumer = Consumer({**config, "group.id": group, "enable.auto.commit": False,
                                  "enable.auto.offset.store": False, "auto.offset.reset": "earliest",
                                  "max.poll.interval.ms": 300000})
        self.consumer.subscribe([topic])

    def publish_one(self, dsn, clock, checkpoint):
        with store.connect(dsn) as db:
            row = store.one(db, """SELECT * FROM event_outbox WHERE sent_at IS NULL
                                   ORDER BY created_at,message_id FOR UPDATE SKIP LOCKED LIMIT 1""")
            if row is None:
                return False
            delivered = []
            self.producer.produce(row["topic"], key=str(row["application_id"]),
                                  value=json.dumps(row["payload"]).encode(),
                                  on_delivery=lambda error, _: delivered.append(error))
            if self.producer.flush(6) or not delivered or delivered[0] is not None:
                raise RuntimeError("Kafka publication unconfirmed")
            checkpoint("after_event_publish", row)
            db.execute("UPDATE event_outbox SET sent_at=%s WHERE message_id=%s",
                       (clock(), row["message_id"]))
        return True

    def consume_one(self, dsn, topic, handler, clock, checkpoint):
        record = self.consumer.poll(0.05)
        if record is None:
            return False
        if record.error():
            raise RuntimeError("Kafka receive failed")
        try:
            with store.connect(dsn) as db:
                receive(db, record, topic, handler, clock)
                checkpoint("before_event_commit", record)
            checkpoint("before_event_ack", record)
            self.consumer.commit(message=record, asynchronous=False)
        except BaseException:
            # При живом процессе следующая попытка должна перечитать откатившуюся запись.
            from confluent_kafka import TopicPartition
            self.consumer.seek(TopicPartition(record.topic(), record.partition(), record.offset()))
            raise
        return True

    def close(self):
        self.consumer.close()


def create_topics(bootstrap=None):
    client = AdminClient({"bootstrap.servers": bootstrap or os.environ["LOANFLOW_KAFKA_BOOTSTRAP"]})
    futures = client.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)
                                   for topic in (REQUEST_TOPIC, RESULT_TOPIC)])
    for future in futures.values():
        try:
            future.result(15)
        except Exception as exc:
            from confluent_kafka import KafkaError, KafkaException
            if not isinstance(exc, KafkaException) or exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
