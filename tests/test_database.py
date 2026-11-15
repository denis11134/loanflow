"""Штатный SQL-тест и сравнение существующих запросов с таблицами главы 2.4."""
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import os
import re
import shlex
import subprocess
import tempfile
import uuid
import psycopg
import pytest
import yaml
from conftest import ROOT

pytestmark = pytest.mark.database


@contextmanager
def temporary_postgres():
    env = {k: v for k, v in os.environ.items() if not k.startswith('PG')}
    bindir = Path(os.environ.get('PG_BINDIR') or subprocess.check_output(['pg_config', '--bindir'], text=True).strip())
    version = subprocess.check_output([str(bindir / 'postgres'), '--version'], text=True)
    assert re.search(r'PostgreSQL\) 18\.', version), version
    with tempfile.TemporaryDirectory(prefix='lf-pg-') as directory:
        base = Path(directory)
        data = base / 'data'
        def run(*args):
            result = subprocess.run([str(bindir / args[0]), *map(str, args[1:])], env=env,
                                    capture_output=True, text=True, timeout=60)
            assert result.returncode == 0, result.stdout + result.stderr + ((base / 'postgres.log').read_text() if (base / 'postgres.log').exists() else '')
            return result
        try:
            run('initdb', '-D', data, '-A', 'trust', '-U', 'loanflow_test', '--no-locale', '-E', 'UTF8')
            # Только отдельный Unix-сокет. Нет TCP и обращения к пользовательскому кластеру.
            compose = yaml.safe_load((ROOT / 'compose.yaml').read_text())
            _, *server_options = compose['services']['postgres']['command']
            options = ['-k', str(base), '-p', '5432', '-c', 'listen_addresses=',
                       '-c', 'unix_socket_permissions=0700', *server_options]
            run('pg_ctl', '-D', data, '-l', base / 'postgres.log', '-o', shlex.join(options), '-w', 'start')
            run('createdb', '-h', base, '-p', '5432', '-U', 'loanflow_test', 'checks')
            yield dict(host=str(base), port=5432, user='loanflow_test', dbname='checks'), run
        finally:
            if (data / 'postmaster.pid').exists():
                run('pg_ctl', '-D', data, '-m', 'immediate', '-w', 'stop')
                assert not (data / 'postmaster.pid').exists(), 'Временный PostgreSQL не остановлен'


def expected_tables():
    """Читаем авторские контрольные таблицы; не генерируем ожидания из результата SQL."""
    text = (ROOT / 'docs/04-data/2.4-sql-practicum.md').read_text()
    result = []
    for section in re.split(r'^## [1-5]\. ', text, flags=re.M)[1:]:
        lines = [line for line in section.splitlines() if line.startswith('|')]
        result.append([[cell.strip().strip('`') for cell in line.strip('|').split('|')]
                       for line in [lines[0], *lines[2:]]])
    assert len(result) == 5
    return result


def assert_cell(actual, expected):
    if expected == 'NULL':
        assert actual is None
    elif isinstance(actual, bool):
        assert str(actual).lower() == expected
    elif isinstance(actual, (int, float, Decimal)):
        assert Decimal(str(actual)) == Decimal(expected)
    elif isinstance(actual, datetime):
        assert actual == datetime.fromisoformat(expected)
    elif isinstance(actual, (date, uuid.UUID)):
        assert str(actual) == expected
    else:
        assert actual == expected


def test_schema_and_five_queries():
    with temporary_postgres() as (params, run):
        run('psql', '-X', '-h', params['host'], '-p', '5432', '-U', params['user'], '-d', params['dbname'],
            '-v', 'ON_ERROR_STOP=1', '-f', ROOT / 'sql/001_initial_schema.sql',
            '-f', ROOT / 'sql/001_initial_schema_test.sql', '-f', ROOT / 'sql/practicum/fixtures.sql')
        queries = re.split(r'^-- [1-5]\s*$', (ROOT / 'sql/practicum/queries.sql').read_text(), flags=re.M)[1:]
        assert len(queries) == 5
        with psycopg.connect(**params) as conn:
            conn.execute("SET TIME ZONE 'Europe/Samara'")
            for number, (query, table) in enumerate(zip(queries, expected_tables()), 1):
                headers, *expected = table
                if number == 1:
                    cases = [(row[0], [] if row[1] == '*0 строк*' else [row[1:]]) for row in expected]
                    headers = headers[1:]
                else:
                    cases = [(None, expected)]
                for customer_id, rows in cases:
                    # Меняется только уже заданный параметр авторского запроса №1.
                    sql = query.replace('10000000-0000-0000-0000-000000000001', customer_id) if customer_id else query
                    cursor = conn.execute(sql)
                    assert [c.name for c in cursor.description] == headers
                    actual = cursor.fetchall()
                    assert len(actual) == len(rows), (number, customer_id, len(actual), len(rows))
                    for actual_row, expected_row in zip(actual, rows):
                        for cell, expected_cell in zip(actual_row, expected_row):
                            assert_cell(cell, expected_cell)


def test_database_stops_on_error():
    with pytest.raises(RuntimeError, match='intentional'):
        with temporary_postgres() as (params, _):
            socket_dir = Path(params['host'])
            assert (socket_dir / 'data/postmaster.pid').exists()
            raise RuntimeError('intentional')
    assert not socket_dir.exists()
