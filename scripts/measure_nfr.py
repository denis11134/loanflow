#!/usr/bin/env python3
"""Последовательная приёмка 5.5; неполный отчёт никогда не означает успех."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
from threading import Lock
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT/"tests")]
from nfr_support import Runtime, until
from services.loanflow import event_bus

COMPONENTS = ("api", "core", "assessment", "kafka", "camunda", "core_database", "assessment_database")


def resources():
    mem = {key: int(value.strip().split()[0]) for key, value in
           (line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())}
    usage = shutil.disk_usage(ROOT)
    temporary = shutil.disk_usage("/tmp")
    return {"cpu_count": os.cpu_count(), "memory_total_mb": mem["MemTotal"]//1024,
            "memory_available_mb": mem["MemAvailable"]//1024,
            "swap_used_mb": (mem["SwapTotal"]-mem["SwapFree"])//1024,
            "disk_free_mb": usage.free//2**20, "tmp_free_mb": temporary.free//2**20,
            "load": list(os.getloadavg())}


def percentile(values, q):
    assert values, "Нет измерений"
    return sorted(values)[max(0, math.ceil(len(values)*q)-1)]


def distribution(values):
    return {"count": len(values), "p50_seconds": percentile(values,.5), "p90_seconds": percentile(values,.9),
            "p95_seconds": percentile(values,.95), "p99_seconds": percentile(values,.99), "max_seconds": max(values)}


def api_measurement(runtime, drafts):
    result = {"rate": 10, "duration_seconds": 180, "planned": 1800, "missed": 0, "errors": 0,
              "max_inflight": 0, "max_dispatch_delay_seconds": 0, "samples": {"GET": [], "PATCH": []}}
    guard, inflight = Lock(), 0

    def request(index):
        nonlocal inflight
        method = "PATCH" if index % 5 == 4 else "GET"
        actor, app = drafts[index % len(drafts)]
        body = {"requested_term_months": 12} if method == "PATCH" else None
        with guard:
            inflight += 1
            result["max_inflight"] = max(result["max_inflight"], inflight)
        try:
            status, _, elapsed = runtime.http(method, "/applications/"+app, actor, body,
                                               key=uuid4() if method == "PATCH" else None)
            with guard:
                result["samples"][method].append(elapsed)
                result["errors"] += status != 200
        except (OSError, ValueError):
            with guard:
                result["errors"] += 1
        finally:
            with guard:
                inflight -= 1

    pending = []
    with ThreadPoolExecutor(10) as pool:
        started = time.monotonic()
        for index in range(1800):
            due = started+index/10
            time.sleep(max(0, due-time.monotonic()))
            late = time.monotonic()-due
            result["max_dispatch_delay_seconds"] = max(result["max_dispatch_delay_seconds"], late)
            pending = [future for future in pending if not future.done()]
            if late >= .1 or len(pending) >= 10:
                result["missed"] += 1
                continue
            pending.append(pool.submit(request, index))
        for future in pending:
            future.result()
    result["elapsed_seconds"] = time.monotonic()-started
    result["distributions"] = {method: distribution(values) for method, values in result["samples"].items()}
    result["passed"] = (result["missed"] == result["errors"] == 0
        and len(result["samples"]["GET"]) == 1440 and len(result["samples"]["PATCH"]) == 360
        and result["max_inflight"] <= 10
        and all(value["p95_seconds"] <= 1 for value in result["distributions"].values()))
    return result


def automatic_measurement(runtime, drafts):
    submits, durations, finishes = [], [], []
    submission_latencies = []
    guard = Lock()

    def evaluate(item):
        actor, app = item
        started = time.monotonic()
        code, _, elapsed = runtime.http("POST", f"/applications/{app}/submission", actor)
        assert code == 202, f"Не принята заявка: HTTP {code}"
        with guard:
            submits.append(started)
            submission_latencies.append(elapsed)
        deadline = started+300
        while True:
            code, body, _ = runtime.http("GET", "/applications/"+app, actor)
            assert code == 200, "Недоступен результат авторешения"
            if body["status"] != "SCORING":
                assert body["status"] == runtime.expected[actor], "Неожиданный исход автоматической оценки"
                finished = time.monotonic()
                with guard:
                    durations.append(finished-started)
                    finishes.append(finished)
                return
            assert time.monotonic() < deadline, "Не завершилась автоматическая оценка"
            time.sleep(.5)

    with ThreadPoolExecutor(5) as pool:
        list(pool.map(evaluate, drafts))
    result = {"submitted": len(submits), "completed": len(durations), "max_concurrent": 5,
              "poll_interval_seconds": .5, "distribution": distribution(durations),
              "submission_response": distribution(submission_latencies),
              "tail_seconds": max(finishes)-max(submits), "samples_seconds": durations}
    result["passed"] = len(durations) == 100 and result["distribution"]["p90_seconds"] < 180 and result["tail_seconds"] <= 300
    assert_effects(runtime, [app for _, app in drafts], sum(runtime.expected[actor] == "APPROVED" for actor, _ in drafts))
    return result


def warm_pipeline(runtime, items):
    for actor, app in items:
        assert runtime.http("POST", f"/applications/{app}/submission", actor)[0] == 202
    def finished():
        return all(runtime.http("GET", "/applications/"+app, actor)[1]["status"] == runtime.expected[actor]
                   for actor, app in items)
    until(finished, 180, "Не завершён прогрев конвейера")


def assert_effects(runtime, applications, approvals):
    params = (applications,)
    for table in ("final_decision", "evaluation", "assessment_dispatch"):
        rows = runtime.sql(f"SELECT count(*) AS count FROM {table} WHERE application_id=ANY(%s::uuid[])", params)
        assert rows[0]["count"] == len(applications), f"Потеря или повтор бизнес-результата: {table}"
    assert runtime.sql("""SELECT count(*) AS count FROM mpl_reservation JOIN offer USING(offer_id)
                          WHERE application_id=ANY(%s::uuid[])""", params)[0]["count"] == approvals
    for role in ("core", "assessment"):
        assert runtime.sql("SELECT count(*) AS count FROM event_outbox WHERE application_id=ANY(%s::uuid[])", params, role)[0]["count"] == len(applications)
    assert runtime.sql("SELECT count(*) AS count FROM assessment_task WHERE application_id=ANY(%s::uuid[]) AND stage='COMPLETED'",
                       params,"assessment")[0]["count"] == len(applications)


def redeliver(runtime, applications):
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": runtime.bootstrap, "log_level": 0})
    sent = []
    for role in ("core", "assessment"):
        for row in runtime.sql("SELECT topic,payload FROM event_outbox WHERE application_id=ANY(%s::uuid[])", (applications,), role):
            payload = row["payload"]
            producer.produce(row["topic"], key=payload["application_id"], value=json.dumps(payload).encode(),
                             on_delivery=lambda error, _: sent.append(error))
    assert producer.flush(5) == 0 and len(sent) == 2*len(applications) and not any(sent)
    until(lambda: runtime.ready() and all(
        runtime.http("GET", "/internal/diagnostics", port=port, monitor=True)[1]["metrics"].find("loanflow_kafka_lag 0\n") >= 0
        for port in (runtime.core_monitor_port, runtime.assessment_monitor_port)), 20, "Повторы не обработаны")


def failure_measurement(runtime, component, offset):
    # Все 10 команд подтверждены API до остановки исполнителя.
    items = [(f"nfr{index}", runtime.draft(f"nfr{index}")) for index in range(offset, offset+10)]
    # Оценка остановлена SIGSTOP только на время постановки очереди, не на время измерения восстановления.
    evaluator = runtime.processes["assessment"]
    os.kill(evaluator.pid, signal_number("STOP"))
    try:
        for actor, app in items:
            assert runtime.http("POST", f"/applications/{app}/submission", actor)[0] == 202
        if component in ("core", "assessment"):
            (runtime.directory/(component+".arm")).write_text("armed")
            os.kill(evaluator.pid, signal_number("CONT"))
            until(lambda: (runtime.directory/(component+".reached")).exists(), 30,
                  "Не достигнута граница подтверждения задания")
        began = time.monotonic()
        runtime.crash(component)
    finally:
        if evaluator.poll() is None:
            os.kill(evaluator.pid, signal_number("CONT"))
    # До восстановления проверяем диагностику с теми же периодом и бюджетами, что у команды --watch.
    target_port = runtime.api_port if component in ("api", "core_database") else (
        runtime.core_monitor_port if component in ("core", "kafka") else runtime.assessment_monitor_port)
    detected = None
    for tick in (5, 10):
        time.sleep(max(0, began+tick-time.monotonic()))
        try:
            code, payload, _ = runtime.http("GET", "/internal/diagnostics", port=target_port, monitor=True)
            failed = code != 200 or not payload["workflow_ready"]
        except (OSError, ValueError):
            failed = True
        if failed and detected is None:
            detected = time.monotonic()-began
    startup = time.monotonic()
    runtime.restore(component)
    ready_at = time.monotonic()

    def complete():
        for actor, app in items:
            code, payload, _ = runtime.http("GET", "/applications/"+app, actor)
            if code != 200 or payload["status"] != runtime.expected[actor]:
                return False
        return True

    until(complete, 60, "Очередь не восстановилась за 60 секунд")
    recovered = time.monotonic()-ready_at
    applications = [app for _, app in items]
    approvals = sum(runtime.expected[actor] == "APPROVED" for actor, _ in items)
    assert_effects(runtime, applications, approvals)
    redeliver(runtime, applications)
    assert_effects(runtime, applications, approvals)
    return {"component": component, "accepted": 10, "completed": 10, "outage_seconds": startup-began,
            "boundary": "before_ack" if component in ("core", "assessment") else "queued",
            "detection_seconds": detected, "dependency_startup_seconds": ready_at-startup,
            "recovery_seconds": recovered, "lost": 0, "duplicate_business_effects": 0,
            "passed": detected is not None and detected <= 15 and recovered <= 60}


def signal_number(name):
    import signal
    return getattr(signal, "SIG"+name)


def stalled_measurement(runtime):
    app = uuid4()
    # Только синтетическая диагностическая запись: ни задания движка, ни бизнес-решения.
    runtime.sql("""INSERT INTO assessment_task
        (application_id,submitted_at,request_fingerprint,inputs,stage,run_at,engine,launch_ack,progress_at,wait_reason)
        VALUES (%s,now()-interval '179 seconds','x','{}','AML',now()+interval '1 day','camunda',true,
                now()-interval '59 seconds','CORE')""", (app,), "assessment")
    began = time.monotonic()
    try:
        time.sleep(5)
        _, payload, _ = runtime.http("GET", "/internal/diagnostics", port=runtime.assessment_monitor_port, monitor=True)
        assert "loanflow_stalled 1\n" in payload["metrics"], "Не обнаружено зависание"
        elapsed = time.monotonic()-began-1
        assert elapsed <= 15, "Превышен срок обнаружения зависания"
        return {"detection_after_threshold_seconds": elapsed, "passed": True}
    finally:
        runtime.sql("DELETE FROM assessment_task WHERE application_id=%s", (app,), "assessment")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT/"test-results/nfr.json")
    parser.add_argument("--runtime", type=Path, default=ROOT/"test-results/nfr-runtime")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    before = resources()
    report = {"schema": 1, "complete": False, "passed": False,
        "sha": subprocess.check_output(["git","rev-parse","HEAD"], cwd=ROOT,text=True).strip(),
        "started_at": datetime.now(timezone.utc).isoformat(), "environment": {"python": platform.python_version(),
        "platform": platform.platform(), "versions": {name: importlib.metadata.version(name) for name in
        ("fastapi","uvicorn","psycopg","confluent-kafka")}, "kafka": "4.1.1", "camunda": "8.10.9",
        "camunda_job_lease_ms": 30000, "resources": before,
        "cpu_model": next((line.split(":",1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                           if line.startswith("model name")), "unknown"),
        "java": subprocess.run(["java","-version"],capture_output=True,text=True,check=True).stderr.splitlines()[0],
        "camunda_java_opts": os.environ.get("LOANFLOW_CAMUNDA_JAVA_OPTS",
            "-Xms128m -Xmx768m -XX:MaxDirectMemorySize=128m -XX:ActiveProcessorCount=2"),
        "kafka_heap": "-Xms128m -Xmx256m", "fixture_version": "nfr-v1-1000-20pct-aml", "warmup_seconds": 30}, "runs": []}

    def save():
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n")

    try:
        save()
        assert before["memory_available_mb"] >= 4096, "Недостаточно RAM для тяжёлой приёмки (нужно 4096 MiB available)"
        assert before["disk_free_mb"] >= 4096 and before["tmp_free_mb"] >= 1024, "Недостаточно диска для приёмки"
        assert not args.runtime.exists(), "Каталог измерений уже существует; выберите новый --runtime"
        for iteration in range(1,4):
            result = {"iteration": iteration, "complete": False, "resources_before": resources(), "failures": []}
            report["runs"].append(result)
            save()
            with Runtime(args.runtime/str(iteration)) as runtime:
                result["cold_start_seconds"] = runtime.cold_start_seconds
                result["postgres_version"] = runtime.sql("SHOW server_version")[0]["server_version"]
                drafts = [(f"nfr{i}", runtime.draft(f"nfr{i}")) for i in range(1000)]
                # Эти четыре клиента участвуют в чтениях, но не в PATCH (индекс %5 !=4).
                warm_pipeline(runtime, drafts[990:994])
                warm_until = time.monotonic()+30
                while time.monotonic() < warm_until:
                    assert runtime.http("GET", "/applications/"+drafts[0][1], drafts[0][0])[0] == 200
                    time.sleep(.1)
                result["api"] = api_measurement(runtime, drafts)
                save()
                assert result["api"]["passed"], "Не пройдены цели API"
                result["automatic"] = automatic_measurement(runtime, drafts[:100])
                save()
                assert result["automatic"]["passed"], "Не пройдены цели автооценки"
                result["stalled"] = stalled_measurement(runtime)
                for number, component in enumerate(COMPONENTS):
                    failure = failure_measurement(runtime, component, 100+number*10)
                    result["failures"].append(failure)
                    save()
                    assert failure["passed"], "Не пройдена цель восстановления/обнаружения"
                result["complete"] = True
                result["resources_after"] = resources()
            save()
        report["complete"] = True
        report["passed"] = all(run["complete"] for run in report["runs"]) and len(report["runs"]) == 3
    except BaseException as error:
        report["failure_category"] = type(error).__name__
        # Только наши фиксированные пояснения, без чужих сообщений/параметров.
        if isinstance(error, AssertionError):
            report["failure_check"] = str(error)
        raise
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["resources_after"] = resources()
        save()


if __name__ == "__main__":
    main()
