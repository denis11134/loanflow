"""Общие пути и явная блокировка готовности при известных дефектах."""
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]


def pytest_addoption(parser):
    parser.addoption('--require-kafka', action='store_true', help='Требовать живой Kafka для приёмки 5.3')
    parser.addoption('--require-ready', action='store_true',
                     help='Завершаться с кодом 1 при ожидаемых дефектах контрактов')


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    reporter = session.config.pluginmanager.getplugin('terminalreporter')
    if session.config.getoption('--require-ready') and reporter.stats.get('xfailed'):
        session.exitstatus = 1


def pytest_terminal_summary(terminalreporter):
    count = len(terminalreporter.stats.get('xfailed', []))
    if count:
        terminalreporter.write_sep('!', f'ГОТОВНОСТЬ ЗАБЛОКИРОВАНА: {count} примеров дефектов контрактов', red=True)
        terminalreporter.write_line('Причины указаны в XFAIL; решения аналитика — docs/verification.md.')
