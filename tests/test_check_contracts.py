import json
import re

import pytest

from api_support import ROOT, document, response_schema, validator
from services.loanflow.checks import parse_passport

PAGE = ROOT / "docs/06-integrations/4.2-field-mappings.md"


def examples(section, language):
    chapter = PAGE.read_text().split(f"## {section}\n", 1)[1].split("\n## ", 1)[0]
    return re.findall(rf"```{language}\n(.*?)\n```", chapter, re.DOTALL)


@pytest.mark.contracts
@pytest.mark.parametrize(("section", "path"), [("КБКИ", "/credit-reports"), ("Антифрод", "/fraud-checks")])
def test_rest_mapping_examples(section, path):
    request, response = map(json.loads, examples(section, "json"))
    doc = document("checks")
    operation = doc["paths"][path]["post"]
    validator(doc, operation["requestBody"]["content"]["application/json"]["schema"]).validate(request)
    validator(doc, response_schema(doc, operation["operationId"], 200)).validate(response)
    assert request["customer_id"] == response["customer_id"]


@pytest.mark.contracts
def test_soap_mapping_examples():
    request, response = examples("Паспорт", "xml")
    request_fields = parse_passport(request.encode(), "CheckPassportRequest")
    response_fields = parse_passport(response.encode(), "CheckPassportResponse")
    assert request_fields["customer_id"] == response_fields["customer_id"]
