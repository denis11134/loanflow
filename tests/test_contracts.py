"""Проверяются тела сообщений, а не только корректность OpenAPI-документа."""
import json
import pytest
from api_support import ROOT, document, response_schema, validator

CASES = json.loads((ROOT / 'tests/contracts/cases.json').read_text())
REASONS = {
    'API-01': 'Терминальная попытка/исчерпанный счётчик допускает действия; FR-UC01-01…04',
    'API-02': 'Допуск закрытия не связан с чтением и принятой командой; 2.3, UC-07',
    'API-03': 'Строковый pattern пропускает сумму выше заявленного bigint; 2.3',
    'API-04': 'Статус Заявки не ограничивает действия и обязательные данные; v3, UC-02',
    'API-05': 'Статус Договора не ограничивает отказ и моменты времени; FR-UC01-05/06/09',
    'API-06': 'Ручная команда, решение, статус и причина не связаны; FR-UC04-01, 2.2',
}


@pytest.mark.contracts
@pytest.mark.parametrize('case', [pytest.param(c, id=c['id'], marks=(
    pytest.mark.xfail(strict=True, raises=AssertionError, reason=c['issue'] + ': ' + REASONS[c['issue']])
    if 'issue' in c else [])) for c in CASES])
def test_contract_example(case):
    doc = document(case['api'])
    schema = (response_schema(doc, case['operation'], case['status']) if 'operation' in case
              else doc['components']['schemas'][case['schema']])
    errors = list(validator(doc, schema).iter_errors(case['body']))
    assert (not errors) == case['valid'], '\n'.join(e.message for e in errors) or 'Недопустимое тело прошло схему'
