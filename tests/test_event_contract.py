"""Проверки примеров и границ данных сообщений оценки."""
from copy import deepcopy

import pytest
import yaml
from jsonschema import Draft7Validator

from api_support import ROOT, UniqueKeysLoader, expanded

pytestmark = pytest.mark.contracts

DOCUMENT = yaml.load(
    (ROOT / "api/assessment.asyncapi.yaml").read_text(),
    Loader=UniqueKeysLoader,
)
MESSAGES = DOCUMENT["components"]["messages"]


def schema_validator(schema):
    schema = expanded(DOCUMENT, schema)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema, format_checker=Draft7Validator.FORMAT_CHECKER)


EXAMPLES = {
    example["name"]: (name, example["payload"])
    for name, message in MESSAGES.items()
    for example in message["examples"]
}


def test_event_channels_from_core_perspective():
    channels = DOCUMENT["channels"]
    operations = DOCUMENT["operations"]
    assert {channel["address"] for channel in channels.values()} == {
        "loanflow.assessment.requests.v1", "loanflow.assessment.results.v1",
    }
    expected = {
        "sendAssessmentRequest": ("send", "assessmentRequests", "assessmentRequested", "AssessmentRequested"),
        "receiveAssessmentResult": ("receive", "assessmentResults", "assessmentCompleted", "AssessmentCompleted"),
    }
    assert operations.keys() == expected.keys()
    for name, (action, channel, message, component) in expected.items():
        assert operations[name]["action"] == action
        assert operations[name]["channel"]["$ref"] == f"#/channels/{channel}"
        assert operations[name]["messages"] == [{"$ref": f"#/channels/{channel}/messages/{message}"}]
        assert channels[channel]["messages"] == {message: {"$ref": f"#/components/messages/{component}"}}


@pytest.mark.parametrize("example_name", EXAMPLES)
def test_event_example(example_name):
    name, payload = EXAMPLES[example_name]
    message = MESSAGES[name]
    schema_validator(message["payload"]).validate(payload)
    schema_validator(message["bindings"]["kafka"]["key"]).validate(payload["application_id"])
    assert expanded(DOCUMENT, message["correlationId"])["location"] == "$message.payload#/application_id"


def test_result_examples_cover_all_outcomes():
    results = [payload["result"] for name, payload in EXAMPLES.values() if name == "AssessmentCompleted"]
    assert {result["outcome"] for result in results} == {
        "RECOMMEND_APPROVAL", "MANUAL_REVIEW", "BUSINESS_REJECTED", "TECHNICAL_FAILURE",
    }
    assert {result["failure_kind"] for result in results if result["outcome"] == "TECHNICAL_FAILURE"} == {
        "PERMANENT", "RETRIES_EXHAUSTED",
    }


@pytest.mark.parametrize(("consent", "payroll_income", "quota_ref"), [
    ({"state": "ABSENT"}, {"state": "UNCONFIRMED"}, None),
    ({"state": "INVALID_SOURCE"}, {"state": "INVALID_SOURCE"}, None),
    ({"state": "GRANTED", "consent_ref": "synthetic-consent"}, {"state": "AVAILABLE", "amount_minor": "0"}, None),
])
def test_source_states_have_valid_representation(consent, payroll_income, quota_ref):
    name, original = EXAMPLES["submittedApplication"]
    payload = deepcopy(original)
    payload["inputs"].update(consent=consent, payroll_income=payroll_income, quota_ref=quota_ref)
    schema_validator(MESSAGES[name]["payload"]).validate(payload)


REMOVE = object()


@pytest.mark.parametrize(("example_name", "path", "value"), [
    ("submittedApplication", ("message_id",), REMOVE),
    ("submittedApplication", ("application_id",), "customer-name"),
    ("submittedApplication", ("submitted_at",), "2026-11-17T08:00:00"),
    ("submittedApplication", ("inputs", "requested_amount_minor"), 5_000_000),
    ("submittedApplication", ("inputs", "requested_amount_minor"), "99999"),
    ("submittedApplication", ("inputs", "requested_amount_minor"), "100000001"),
    ("submittedApplication", ("inputs", "requested_term_months"), 61),
    ("submittedApplication", ("inputs", "rules_version"), REMOVE),
    ("submittedApplication", ("inputs", "payroll_income"), REMOVE),
    ("submittedApplication", ("inputs", "payroll_income"), {"state": "AVAILABLE"}),
    ("submittedApplication", ("inputs", "payroll_income", "amount_minor"), "-1"),
    ("submittedApplication", ("inputs", "consent", "consent_ref"), " "),
    ("submittedApplication", ("inputs", "consent"), {"state": "ABSENT", "consent_ref": "synthetic-ref"}),
    ("submittedApplication", ("inputs", "full_profile"), {"synthetic_marker": True}),
    ("submittedApplication", ("inputs", "credit_report"), "synthetic-report-marker"),
    ("submittedApplication", ("inputs", "account_ref"), "synthetic-account-marker"),
    ("submittedApplication", ("authorization",), "synthetic-token-marker"),
    ("submittedApplication", ("sms_code",), "123456"),
    ("recommendApproval", ("result", "outcome"), "APPROVED"),
    ("recommendApproval", ("result", "quota_ref", "bucket"), REMOVE),
    ("recommendApproval", ("result", "psk_limit_exceeded"), REMOVE),
    ("recommendApproval", ("result", "terms", "psk_annual_percent"), "20.000"),
    ("recommendApproval", ("result", "terms", "psk_rub"), 5580.70),
    ("manualReview", ("result", "terms"), REMOVE),
    ("manualReview", ("result", "pdn_notice_required"), REMOVE),
    ("manualReview", ("result", "pdn_percent"), "53.88"),
    ("selfBan", ("result", "reason_code"), REMOVE),
    ("selfBan", ("result", "reason_code"), "MPL_QUOTA_EXHAUSTED"),
    ("invalidSource", ("result", "error_body"), "synthetic-error-marker"),
    ("checksTimedOut", ("result", "failure_kind"), "TIMEOUT"),
], ids=[
    "missing-message-id", "invalid-application-id", "timestamp-without-offset", "numeric-amount",
    "amount-below-minimum", "amount-above-maximum", "term-above-maximum", "missing-rules-version",
    "missing-income", "available-income-without-amount", "negative-income", "blank-consent",
    "unexpected-consent-ref", "full-profile", "credit-report", "account-ref", "authorization", "sms-code",
    "final-status-as-recommendation", "missing-quota-bucket", "missing-psk-flag", "short-psk",
    "numeric-psk-rub", "manual-without-terms", "missing-pdn-flag", "numeric-pdn",
    "missing-rejection-reason", "core-only-rejection-reason", "raw-error", "nonterminal-timeout",
])
def test_event_rejects_invalid_message(example_name, path, value):
    name, original = EXAMPLES[example_name]
    payload = deepcopy(original)
    target = payload
    for field in path[:-1]:
        target = target[field]
    if value is REMOVE:
        del target[path[-1]]
    else:
        target[path[-1]] = value
    assert not schema_validator(MESSAGES[name]["payload"]).is_valid(payload)
