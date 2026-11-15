import json
import os
from functools import lru_cache
from http.client import HTTPException
from urllib.error import HTTPError
from urllib.request import Request, build_opener
from uuid import UUID

from jsonschema import ValidationError
from lxml import etree

from .http import NoRedirect, document, validator
from .store import ROOT

REST_PATHS = {"CREDIT_REPORT": "/credit-reports", "FRAUD": "/fraud-checks"}
SOAP = "http://schemas.xmlsoap.org/soap/envelope/"
PASSPORT = "urn:loanflow:training:passport:v1"
SOAP_ACTION = f'"{PASSPORT}#CheckPassport"'
SOAP_TYPE = "text/xml; charset=utf-8"
CHECK_STAGES = ("PASSPORT", *REST_PATHS)


@lru_cache(maxsize=None)
def passport_schema():
    return etree.XMLSchema(etree.parse(str(ROOT / "api/passport.xsd")))


def soap_envelope(operation):
    envelope = etree.Element(f"{{{SOAP}}}Envelope", nsmap={"soap": SOAP, "p": PASSPORT})
    body = etree.SubElement(envelope, f"{{{SOAP}}}Body")
    return envelope, etree.SubElement(body, operation)


def passport_message(operation, fields):
    envelope, element = soap_envelope(f"{{{PASSPORT}}}{operation}")
    for name, value in fields.items():
        etree.SubElement(element, f"{{{PASSPORT}}}{name}").text = value
    return etree.tostring(envelope, encoding="utf-8")


def parse_passport(raw, operation):
    parser = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True,
                             remove_comments=True, remove_pis=True)
    envelope = etree.fromstring(raw, parser)
    if envelope.getroottree().docinfo.doctype or envelope.tag != f"{{{SOAP}}}Envelope":
        raise ValueError("Invalid SOAP envelope")
    elements = list(envelope)
    if elements and elements[0].tag == f"{{{SOAP}}}Header":
        elements = elements[1:]
    if len(elements) != 1 or elements[0].tag != f"{{{SOAP}}}Body" or len(elements[0]) != 1:
        raise ValueError("Invalid SOAP body")
    body = elements[0]
    element = body[0]
    if (body.text or "").strip() or (element.tail or "").strip() or element.tag != f"{{{PASSPORT}}}{operation}":
        raise ValueError("Unexpected SOAP operation")
    passport_schema().assertValid(element)
    return {etree.QName(field).localname: field.text for field in element}


class ChecksClient:
    def __init__(self, url=None):
        self.url = url
        self.opener = build_opener(NoRedirect())
        doc = document("checks")
        self.responses = {
            kind: validator(doc, doc["paths"][path]["post"]["responses"]["200"]["content"]["application/json"]["schema"])
            for kind, path in REST_PATHS.items()
        }

    def call(self, kind, customer_id, consent_ref=None):
        url = self.url or os.environ["LOANFLOW_CHECKS_URL"]
        fields = {"customer_id": str(customer_id)}
        if kind == "PASSPORT":
            path = "/passport-check"
            body = passport_message("CheckPassportRequest", fields)
            headers = {"Content-Type": SOAP_TYPE, "SOAPAction": SOAP_ACTION}
            content_type = "text/xml"
        else:
            path = REST_PATHS[kind]
            if kind == "CREDIT_REPORT":
                fields["consent_ref"] = consent_ref
            body = json.dumps(fields).encode()
            content_type = "application/json"
            headers = {"Content-Type": content_type}

        request = Request(url + path, data=body, headers=headers, method="POST")
        try:
            with self.opener.open(request, timeout=2) as response:
                if response.status == 503:
                    return "TEMPORARY", None
                if response.status != 200 or response.headers.get_content_type() != content_type:
                    return "PERMANENT", None
                raw = response.read()
        except HTTPError as exc:
            with exc:
                return ("TEMPORARY" if exc.code == 503 else "PERMANENT"), None
        except (OSError, HTTPException):
            return "TEMPORARY", None

        try:
            if kind == "PASSPORT":
                result = parse_passport(raw, "CheckPassportResponse")
            else:
                result = json.loads(raw)
                self.responses[kind].validate(result)
            if UUID(result["customer_id"]) != customer_id:
                return "PERMANENT", None
        except (ValueError, ValidationError, etree.LxmlError):
            return "PERMANENT", None
        return "OK", result
