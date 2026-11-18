"""Локальный Apache Kafka в отдельном процессе, без Docker."""
from contextlib import contextmanager
from pathlib import Path
import os
import signal
import socket
import subprocess
import time
from uuid import uuid4

from confluent_kafka.admin import AdminClient


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextmanager
def kafka_server(directory):
    home = Path(os.environ["KAFKA_HOME"])
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    port, controller = free_port(), free_port()
    config = directory / "server.properties"
    config.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={directory / 'data'}
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
num.partitions=3
auto.create.topics.enable=false
""")
    env = dict(os.environ, KAFKA_HEAP_OPTS="-Xms128m -Xmx256m", LOG_DIR=str(directory / "logs"))
    subprocess.run([str(home / "bin/kafka-storage.sh"), "format", "-t", str(uuid4()).replace("-", "")[:22],
                    "-c", str(config)], env=env, check=True, capture_output=True, timeout=60)
    with (directory / "broker.log").open("w") as log:
        proc = subprocess.Popen([str(home / "bin/kafka-server-start.sh"), str(config)],
                                env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            bootstrap = f"127.0.0.1:{port}"
            admin = AdminClient({"bootstrap.servers": bootstrap, "log_level": 0})
            deadline = time.monotonic() + 45
            while True:
                if proc.poll() is not None:
                    raise RuntimeError("Kafka stopped during startup")
                try:
                    if admin.list_topics(timeout=1).brokers:
                        break
                except Exception:
                    pass
                if time.monotonic() > deadline:
                    raise RuntimeError("Kafka startup timed out")
            yield bootstrap
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(15)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(5)
