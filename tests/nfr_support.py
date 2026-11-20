"""Настоящие процессы и изолированные хранилища для последовательной приёмки 5.5."""
from contextlib import ExitStack
from copy import deepcopy
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from uuid import uuid4

import psycopg
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

from services.loanflow import assessment_service, event_bus, store
from services.loanflow.seed import DEMO, seed
from camunda_support import camunda_server
from kafka_support import kafka_server, free_port
from test_database import temporary_postgres


def until(check, timeout, message):
    deadline = time.monotonic()+timeout
    while True:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError, psycopg.Error, KafkaException):
            pass
        if time.monotonic() >= deadline:
            raise AssertionError(message)
        time.sleep(.1)


class Runtime:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.stack = ExitStack()
        self.processes = {}
        self.commands = {}
        self.environments = {}
        self.database_controls = {}
        self.engine_context = None
        self.cold_start_seconds = None

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.saved_env = dict(os.environ)
        try:
            for name in ("core", "assessment"):
                params, run = self.stack.enter_context(temporary_postgres())
                self.database_controls[name] = (params, run)
                setattr(self, name+"_dsn", psycopg.conninfo.make_conninfo(**params))
            os.environ["LOANFLOW_SECRET"] = "42"*32
            store.migrate(self.core_dsn)
            assessment_service.migrate(self.assessment_dsn)
            data = json.loads(DEMO.read_text())
            prototype = deepcopy(data["customers"]["mobile"])
            data["customers"] = {}
            data["actors"] = {}
            data["quotas"][0]["capacity"] = 5000
            self.expected = {}
            # Фиксированный набор: 1000 клиентов, 20% отказов по AML, остальные одобряются.
            for index in range(1000):
                name = f"nfr{index}"
                item = deepcopy(prototype)
                item.update(customer_id=f"20000000-0000-0000-0000-{index+1:012d}",
                            account_ref=f"nfr-account-{index}", profile_ref="nfr-profile-v1")
                item["scenario"].update(payroll_income_rub="100000.00", existing_payments_rub="0.00",
                                        aml_block=index % 5 == 0)
                data["customers"][name] = item
                data["actors"][name] = {"role": "CUSTOMER", "customer": name}
                self.expected[name] = "BANK_REJECTED" if index % 5 == 0 else "APPROVED"
            path = self.directory / "fixtures.json"
            path.write_text(json.dumps(data))
            self.tokens = seed(path, self.core_dsn)
            self.broker_control = {}
            self.bootstrap = self.stack.enter_context(kafka_server(self.directory/"kafka", self.broker_control))
            event_bus.create_topics(self.bootstrap)
            start = time.monotonic()
            self.start_engine()
            self.cold_start_seconds = time.monotonic()-start
            self.api_port, self.checks_port, self.core_monitor_port, self.assessment_monitor_port = (free_port() for _ in range(4))
            self.env = dict(os.environ, LOANFLOW_DSN=self.core_dsn, LOANFLOW_SECRET="42"*32,
                            LOANFLOW_ASSESSMENT_SECRET="43"*32,
                            LOANFLOW_MONITOR_TOKEN="synthetic-nfr-monitor-secret",
                            LOANFLOW_ASSESSMENT_TOKEN="synthetic-nfr-assessment-secret",
                            LOANFLOW_ABS_TOKEN="synthetic-nfr-abs-secret",
                            LOANFLOW_ABS_URL="http://127.0.0.1:1",
                            LOANFLOW_CHECKS_URL=f"http://127.0.0.1:{self.checks_port}",
                            LOANFLOW_CORE_URL=f"http://127.0.0.1:{self.api_port}",
                            LOANFLOW_KAFKA_BOOTSTRAP=self.bootstrap, LOANFLOW_ASSESSMENT_ENGINE="camunda",
                            LOANFLOW_CAMUNDA_URL=self.engine_url, LOANFLOW_CAMUNDA_JOB_TIMEOUT_MS="30000",
                            LOANFLOW_CAMUNDA_MANAGEMENT_URL="http://127.0.0.1:9600")
            # Родитель тоже использует настоящий брокер для проверки повторной доставки.
            os.environ.update({key: value for key, value in self.env.items() if key.startswith("LOANFLOW_")})
            for name, module, port in (("api", "api", self.api_port), ("checks", "check_stubs", self.checks_port)):
                command = ["-m", "uvicorn", f"services.loanflow.{module}:app", "--host", "127.0.0.1",
                           "--port", str(port), "--no-access-log", "--log-level", "critical"]
                self.start(name, command, self.env)
            self.start("core", ["tests/nfr_worker.py", "core", str(self.directory)],
                       dict(self.env, LOANFLOW_MONITOR_PORT=str(self.core_monitor_port)))
            assessment_env = dict(self.env, LOANFLOW_DSN=self.assessment_dsn, LOANFLOW_SECRET="43"*32,
                                  LOANFLOW_MONITOR_PORT=str(self.assessment_monitor_port))
            assessment_env.pop("LOANFLOW_ABS_TOKEN", None)
            self.start("assessment", ["tests/nfr_worker.py", "assessment", str(self.directory)], assessment_env)
            until(self.ready, 30, "Не готов учебный конвейер")
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def start_engine(self):
        self.engine_context = camunda_server(self.directory/"camunda")
        try:
            self.engine_url = self.engine_context.__enter__()
        except BaseException:
            self.engine_context = None
            raise

    def start(self, name, command=None, env=None):
        if command is not None:
            self.commands[name], self.environments[name] = command, env
        output = self.stack.enter_context((self.directory/(name+".log")).open("a"))
        self.processes[name] = subprocess.Popen([sys.executable, *self.commands[name]], cwd=store.ROOT,
            env=self.environments[name], stdout=output, stderr=subprocess.STDOUT)

    def crash(self, component):
        if component in self.processes:
            proc = self.processes[component]
            if proc.poll() is None:
                proc.kill()
                proc.wait(5)
        elif component == "kafka":
            self.broker_control["crash"]()
        elif component.endswith("_database"):
            role = component.removesuffix("_database")
            params, run = self.database_controls[role]
            run("pg_ctl", "-D", Path(params["host"])/"data", "-m", "immediate", "-w", "stop")
        elif component == "camunda":
            marker = ("LOANFLOW_CAMUNDA_DATA="+str(self.directory/"camunda/camunda-data")).encode()
            killed = 0
            for path in Path("/proc").iterdir():
                if not path.name.isdigit():
                    continue
                try:
                    if ((path/"comm").read_text().strip() == "java"
                            and marker in (path/"environ").read_bytes().split(b"\0")):
                        os.kill(int(path.name), signal.SIGKILL)
                        killed += 1
                except (OSError, PermissionError):
                    continue
            assert killed, "Не найден собственный процесс Camunda"
            self.engine_context.__exit__(None, None, None)
            self.engine_context = None

    def restore(self, component):
        if component in self.processes:
            self.start(component)
        elif component == "kafka":
            self.broker_control["start"]()
            until(lambda: bool(AdminClient({"bootstrap.servers": self.bootstrap,"log_level": 0}).list_topics(timeout=1).brokers),
                  45, "Не восстановился Kafka")
        elif component.endswith("_database"):
            params, run = self.database_controls[component.removesuffix("_database")]
            run("pg_ctl", "-D", Path(params["host"])/"data", "-l", Path(params["host"])/"postgres.log", "-w", "start")
        elif component == "camunda":
            self.start_engine()
        until(self.ready, 30, "Зависимости не готовы после восстановления")

    def http(self, method, path, actor="nfr0", body=None, port=None, monitor=False, key=None):
        conn = HTTPConnection("127.0.0.1", port or self.api_port, timeout=2)
        headers = {"Authorization": "Bearer "+(self.env["LOANFLOW_MONITOR_TOKEN"] if monitor else self.tokens[actor])}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if key is not None:
            headers["Idempotency-Key"] = str(key)
        try:
            started = time.monotonic()
            conn.request(method, path, json.dumps(body) if body is not None else None, headers)
            response = conn.getresponse()
            raw = response.read()
            elapsed = time.monotonic()-started
            return response.status, json.loads(raw), elapsed
        finally:
            conn.close()

    def ready(self):
        for port in (self.api_port, self.core_monitor_port, self.assessment_monitor_port):
            status, payload, _ = self.http("GET", "/internal/diagnostics", port=port, monitor=True)
            if status != 200 or not payload["workflow_ready"]:
                return False
        return True

    def sql(self, query, params=(), role="core"):
        with store.connect(getattr(self, role+"_dsn")) as db:
            cursor = db.execute(query, params)
            return cursor.fetchall() if cursor.description else None

    def draft(self, actor):
        code, body, _ = self.http("POST", "/applications", actor,
            {"requested_amount_minor": "5000000", "requested_term_months": 12}, key=uuid4())
        assert code == 201, f"Не создан синтетический черновик: HTTP {code}"
        return body["application_id"]

    def __exit__(self, *exc):
        for proc in self.processes.values():
            if proc.poll() is None:
                # Барьер SIGSTOP при неуспешном тесте не должен мешать завершению.
                proc.send_signal(signal.SIGCONT)
                proc.terminate()
                try:
                    proc.wait(10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(5)
        try:
            if self.engine_context is not None:
                self.engine_context.__exit__(*exc)
                self.engine_context = None
        finally:
            self.stack.close()
            os.environ.clear()
            os.environ.update(self.saved_env)
