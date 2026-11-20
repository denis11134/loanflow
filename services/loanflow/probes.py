"""Изолированные проверки с общим бюджетом, включая DNS, чтение тела и завершение."""
import asyncio
import json
import os
import sys
import time
from urllib.request import HTTPRedirectHandler, Request, build_opener


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ProbeUnavailable(Exception):
    pass


async def run_probe(action, values, timeout=1.3):
    process = None
    try:
        async with asyncio.timeout(timeout):
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "services.loanflow.probes", stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            raw, _ = await process.communicate(json.dumps({"action": action, "values": values}).encode())
            if process.returncode != 0 or len(raw) > 65536:
                raise ProbeUnavailable
            result = json.loads(raw)
            if result.get("ok") is not True:
                raise ProbeUnavailable
            return result["values"]
    except (TimeoutError, OSError, ValueError, KeyError):
        raise ProbeUnavailable from None
    finally:
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()


def database_snapshot(dsn, role):
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(
        dsn or os.environ["LOANFLOW_DSN"], row_factory=dict_row, connect_timeout=2,
        options="-c statement_timeout=800 -c lock_timeout=500 -c default_transaction_read_only=on",
    ) as db:
        def rows(query, params=()):
            return db.execute(query, params).fetchall()

        # Отсутствующая миграция также означает неготовность, а не успешный SELECT 1.
        metrics = {}
        outbox, = rows("""SELECT count(*) AS count, coalesce(greatest(0,
            extract(epoch FROM now()-min(created_at))),0)::float AS oldest_seconds
            FROM event_outbox WHERE sent_at IS NULL""")
        metrics["outbox"] = outbox
        metrics["rejections"] = rows("SELECT category,count(*)::int AS count FROM event_rejection GROUP BY category")
        if role == "assessment":
            metrics["assessments"] = rows("""SELECT stage,count(*)::int AS count FROM assessment_task GROUP BY stage""")
            stalled, = rows("""SELECT count(*)::int AS count FROM assessment_task
                WHERE stage NOT IN ('COMPLETED','CANCELED') AND submitted_at < now()-interval '180 seconds'
                AND progress_at < now()-interval '60 seconds'
                AND (wait_reason IS DISTINCT FROM 'RETRY' OR wait_until IS NULL OR wait_until<=now())""")
            metrics["stalled"] = stalled["count"]
            metrics["waiting"] = rows("""SELECT wait_reason,count(*)::int AS count FROM assessment_task
                WHERE stage NOT IN ('COMPLETED','CANCELED') AND wait_reason IS NOT NULL GROUP BY wait_reason""")
        else:
            # Требуем реальное наличие последней миграции, не только таблицы журнала.
            version = rows("SELECT 1 FROM service_migration WHERE name='011_observability.sql'")
            if not version:
                raise RuntimeError("Schema not ready")
            metrics["jobs"] = rows("""SELECT kind,count(*)::int AS count,coalesce(greatest(0,
                extract(epoch FROM now()-min(run_at))),0)::float AS oldest_seconds
                FROM service_job WHERE run_at<=now() GROUP BY kind""")
            uncertain, = rows("""SELECT count(*)::int AS count,coalesce(greatest(0,
                extract(epoch FROM now()-min(o.started_at))),0)::float AS oldest_seconds
                FROM disbursement_outbox o LEFT JOIN abs_final_result f USING(instruction_id)
                WHERE o.state='STARTED' AND f.instruction_id IS NULL""")
            metrics["uncertain_instructions"] = uncertain
            stale, = rows("""SELECT count(*)::int AS count FROM application
                WHERE status='SCORING' AND submitted_at<now()-interval '180 seconds'""")
            metrics["scoring_overdue"] = stale["count"]
        return metrics



def kafka_snapshot(role, group=None):
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec
    REQUEST_TOPIC, RESULT_TOPIC = "loanflow.assessment.requests.v1", "loanflow.assessment.results.v1"
    topic = RESULT_TOPIC if role == "core" else REQUEST_TOPIC
    group = group or os.environ.get("LOANFLOW_KAFKA_GROUP", f"loanflow-{role}-v1")
    deadline = time.monotonic() + 1.15

    def remaining():
        return max(.001, deadline - time.monotonic())

    admin = AdminClient({"bootstrap.servers": os.environ["LOANFLOW_KAFKA_BOOTSTRAP"],
                         "log_level": 0, "socket.timeout.ms": 1000})
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


def http_probe(url, token=None, timeout=1.8):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with build_opener(NoRedirect()).open(Request(url, headers=headers), timeout=timeout) as response:
        payload = response.read(65537)
        if len(payload) > 65536:
            raise ValueError("Probe response too large")
        return json.loads(payload)



def main():
    try:
        request = json.loads(sys.stdin.buffer.read(65537))
        action, values = request["action"], request["values"]
        if action == "database":
            result = database_snapshot(**values)
        elif action == "kafka":
            result = kafka_snapshot(**values)
        elif action == "http":
            result = http_probe(**values)
        else:
            raise ProbeUnavailable
        output = json.dumps({"ok": True, "values": result})
        if len(output.encode()) > 65536:
            raise ProbeUnavailable
        print(output)
    except Exception:
        print('{"ok":false}')


if __name__ == "__main__":
    main()
