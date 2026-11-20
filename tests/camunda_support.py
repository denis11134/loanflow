"""Настоящий Camunda 8 Run в отдельном процессе, с сохраняемыми H2 и Zeebe."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
VERSION = "8.10.9"


def camunda_home():
    value = os.environ.get("CAMUNDA_HOME")
    return Path(value).resolve() if value else None


def camunda_configured():
    home = camunda_home()
    return bool(os.environ.get("LOANFLOW_CAMUNDA_TEST_URL") or (home and (home / "c8run").is_file()))


def _free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def _prepare_runtime(home, directory):
    directory.mkdir(parents=True, exist_ok=True)
    for source in home.iterdir():
        target = directory / source.name
        if target.exists() or target.is_symlink() or source.name in {"log", "camunda-data"}:
            continue
        if source.name == f"camunda-zeebe-{VERSION}":
            target.mkdir()
            for component in source.iterdir():
                destination = target / component.name
                if component.name in {"data", "logs"}:
                    continue
                if component.name in {"bin", "config"}:
                    shutil.copytree(component, destination)
                else:
                    destination.symlink_to(component, target_is_directory=component.is_dir())
        elif source.name == "configuration":
            shutil.copytree(source, target)
        elif source.name.endswith((".process", ".lock")):
            continue
        else:
            target.symlink_to(source, target_is_directory=source.is_dir())
    (directory / "log").mkdir(exist_ok=True)
    shutil.copyfile(ROOT / "services/camunda/application.yaml", directory / "loanflow.yaml")


def _wait_ready(url, process=None, timeout=180):
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        if process and process.poll() not in {None, 0}:
            raise RuntimeError(f"Camunda 8 Run завершился с кодом {process.returncode}")
        try:
            with urlopen(f"{url}/v2/topology", timeout=2) as response:
                topology = json.load(response)
            if topology.get("brokers"):
                return
        except (URLError, TimeoutError, ValueError, ConnectionError) as error:
            last_error = str(error)
        time.sleep(0.25)
    raise RuntimeError(f"Camunda 8 Run не готов: {last_error}")


@contextmanager
def camunda_server(directory):
    external = os.environ.get("LOANFLOW_CAMUNDA_TEST_URL")
    if external:
        url = external.rstrip("/")
        _wait_ready(url)
        yield url
        return
    home = camunda_home()
    if not home or not (home / "c8run").is_file():
        raise RuntimeError("Для Camunda нужен CAMUNDA_HOME или LOANFLOW_CAMUNDA_TEST_URL")
    directory = Path(directory).resolve()
    _prepare_runtime(home, directory)
    ports_file = directory / "ports.json"
    if not ports_file.exists():
        port, command, internal, grpc = _free_ports(4)
        # Собственный readiness probe c8run 8.10.9 использует порт 9600.
        ports_file.write_text(json.dumps([port, 9600, command, internal, grpc]))
    port, management, command, internal, grpc = json.loads(ports_file.read_text())
    env = dict(
        os.environ,
        JAVA_OPTS=os.environ.get(
            "LOANFLOW_CAMUNDA_JAVA_OPTS",
            "-Xms128m -Xmx768m -XX:MaxDirectMemorySize=128m -XX:ActiveProcessorCount=2",
        ),
        LOANFLOW_CAMUNDA_DATA=str(directory / "camunda-data"),
        LOANFLOW_CAMUNDA_MANAGEMENT_PORT=str(management),
        LOANFLOW_CAMUNDA_COMMAND_PORT=str(command),
        LOANFLOW_CAMUNDA_INTERNAL_PORT=str(internal),
        LOANFLOW_CAMUNDA_GRPC_PORT=str(grpc),
    )
    url = f"http://127.0.0.1:{port}"
    with (directory / "run.log").open("a") as log:
        process = subprocess.Popen(
            [str(directory / "c8run"), "start", "--no-browser", "--disable-connectors",
             "--port", str(port), "--config", "loanflow.yaml"],
            cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            try:
                _wait_ready(url, process)
            except RuntimeError as error:
                detail = (directory / "log/camunda.log")
                tail = detail.read_text(errors="replace")[-12000:] if detail.exists() else ""
                raise RuntimeError(f"{error}\n{directory / 'run.log'}\n{tail}") from error
            yield url
        finally:
            subprocess.run(
                [str(directory / "c8run"), "stop"], cwd=directory, env=env,
                stdout=log, stderr=subprocess.STDOUT, timeout=90, check=False,
            )
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(5)
