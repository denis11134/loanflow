"""Раздельная проверка документа OAS и экземпляров данных OAS 3.0."""
from functools import lru_cache
from pathlib import Path
import yaml
from openapi_schema_validator import OAS30Validator

ROOT = Path(__file__).resolve().parents[1]
METHODS = {'get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace'}


class UniqueKeysLoader(yaml.SafeLoader):
    pass


def unique_mapping(loader, node):
    entries = loader.construct_pairs(node)
    result = dict(entries)
    if len(entries) != len(result):
        raise ValueError('Повтор ключа YAML')
    return result


UniqueKeysLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)


@lru_cache(maxsize=None)
def document(name):
    return yaml.load((ROOT / 'api' / f'{name}.openapi.yaml').read_text(), Loader=UniqueKeysLoader)


def resolve(doc, value):
    if '$ref' not in value:
        return value
    ref = value['$ref']
    assert ref.startswith('#/'), f'Внешняя ссылка не поддерживается офлайн: {ref}'
    target = doc
    for part in ref[2:].split('/'):
        target = target[part.replace('~1', '/').replace('~0', '~')]
    return target


def expanded(doc, value):
    if isinstance(value, dict):
        # В OAS 3.0 соседние поля Reference Object не переопределяют цель.
        return {k: expanded(doc, v) for k, v in resolve(doc, value).items()}
    if isinstance(value, list):
        return [expanded(doc, v) for v in value]
    return value


def operations(doc):
    for path, item in doc['paths'].items():
        for method, operation in item.items():
            if method in METHODS:
                yield path, method, operation


def validator(doc, schema):
    return OAS30Validator(expanded(doc, schema), format_checker=OAS30Validator.FORMAT_CHECKER)


def response_schema(doc, operation_id, status):
    op = next(o for _, _, o in operations(doc) if o['operationId'] == operation_id)
    return resolve(doc, op['responses'][str(status)])['content']['application/json']['schema']
