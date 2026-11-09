"""Проверяются тела сообщений, а не только корректность OpenAPI-документа."""
import json
import pytest
from api_support import ROOT, document, response_schema, validator

CASES = json.loads((ROOT / 'tests/contracts/cases.json').read_text())


@pytest.mark.contracts
@pytest.mark.parametrize('case', CASES, ids=lambda case: case['id'])
def test_contract_example(case):
    doc = document(case['api'])
    schema = (response_schema(doc, case['operation'], case['status']) if 'operation' in case
              else doc['components']['schemas'][case['schema']])
    errors = list(validator(doc, schema).iter_errors(case['body']))
    assert (not errors) == case['valid'], '\n'.join(e.message for e in errors) or 'Недопустимое тело прошло схему'


@pytest.mark.contracts
def test_amount_pattern_matches_exact_integer_boundaries():
    limit = 9_223_372_036_854_775_807
    doc = document('loanflow')
    check = validator(doc, doc['components']['schemas']['PositiveAmountMinor'])
    # Проверяем соседние числа на границах всех десятичных префиксов.
    for scale in (10 ** i for i in range(19)):
        boundary = limit // scale * scale
        for amount in (boundary - 1, boundary, boundary + 1, boundary + scale - 1, boundary + scale):
            assert check.is_valid(str(amount)) == (1 <= amount <= limit), amount
    assert not check.is_valid(f'{limit}\n')
