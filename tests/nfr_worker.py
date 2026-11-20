"""Исполнитель приёмки: штатный цикл с одноразовым барьером перед подтверждением."""
import logging
import os
from pathlib import Path
import signal
import sys
from threading import Event

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.loanflow.events import EventProcess, run_loop
from services.loanflow.diagnostics import worker_monitor


def main():
    role, directory = sys.argv[1], Path(sys.argv[2])
    arm, reached = directory/(role+".arm"), directory/(role+".reached")

    def checkpoint(name, *_):
        expected = "after_camunda_step_commit" if role == "assessment" else "before_event_ack"
        if name == expected and arm.exists():
            arm.unlink()
            reached.write_text("ready")
            os.kill(os.getpid(), signal.SIGSTOP)

    logging.basicConfig(level=logging.INFO)
    process = EventProcess(role, checkpoint=checkpoint)
    # На стороне оценки checkpoint нужен и в самом исполнителе Camunda.
    process.service.checkpoint = checkpoint
    stopping = Event()
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    try:
        with worker_monitor(role, process.dsn) as monitor:
            run_loop(process, stopping, monitor)
    finally:
        process.close()


if __name__ == "__main__":
    main()
