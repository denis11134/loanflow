import json
import re
from collections import Counter
import pytest
from jsonschema import Draft4Validator
from api_support import ROOT, document, expanded, operations, resolve


@pytest.mark.parametrize('name,expected_count', [('loanflow', 29), ('abs', 3), ('checks', 2)])
def test_openapi_structure(name, expected_count):
    doc = document(name)
    meta = json.loads((ROOT / 'tests/schemas/openapi-3.0-2024-10-18.json').read_text())
    Draft4Validator(meta).validate(doc)
    expanded(doc, doc)  # Проверяет все локальные ссылки, включая неиспользуемые компоненты.
    ids = Counter()
    for path, method, op in operations(doc):
        ids[op['operationId']] += 1
        merged = {}
        for level in (doc['paths'][path].get('parameters', []), op.get('parameters', [])):
            resolved = [resolve(doc, p) for p in level]
            assert len({(p['name'], p['in']) for p in resolved}) == len(resolved)
            merged.update({(p['name'], p['in']): p for p in resolved})
        parameters = list(merged.values())
        assert {p['name'] for p in parameters if p['in'] == 'path'} == set(re.findall(r'{([^{}]+)}', path))
        assert all(p.get('required') is True for p in parameters if p['in'] == 'path')
        assert len({(p['name'], p['in']) for p in parameters}) == len(parameters)
        assert any(str(s).startswith('2') for s in op['responses'])
    assert all(n == 1 for n in ids.values())
    assert len(ids) == expected_count


def test_requirements_roles_and_protocol():
    doc = document('loanflow')
    ops = {o['operationId']: (m, o) for _, m, o in operations(doc)}
    requirements = set(re.findall(r'^\| `(FR-UC\d{2}-\d{2})`',
        (ROOT / 'docs/03-requirements/1.6-mvp-requirements.md').read_text(), re.M))
    assert requirements == {r for _, o in ops.values() for r in o['x-requirements']}
    for op in ('createDraft', 'updateDraft', 'startSigningAttempt', 'resendSigningSms',
               'verifySigningCode', 'recordManualDecision', 'withdrawInContactCenter', 'registerAbsRead'):
        assert {'$ref': '#/components/parameters/idempotency_key'} in ops[op][1]['parameters']
    for method, op in ops.values():
        assert op['x-requirements'] and op['x-roles']
        if {'BRANCH', 'UNDERWRITING_SUPERVISOR'} & set(op['x-roles']):
            assert method == 'get'
    for name in ('ApplicationView', 'ContactApplication', 'ContactContract', 'PresenceCheck', 'UnderwritingView'):
        assert not {'rejection_reason_code', 'scoring_score', 'account_ref'} & doc['components']['schemas'][name]['properties'].keys()
    assert {'200', '202'} <= ops['requestAbsClosure'][1]['responses'].keys()
    assert 'application/pdf' in ops['getIndividualTermsDocument'][1]['responses']['200']['content']
    for name, schema in document('abs')['components']['schemas'].items():
        if name != 'InstructionPayload':
            assert 'instruction_id' in schema['required']
