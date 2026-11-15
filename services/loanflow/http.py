import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from urllib.request import HTTPRedirectHandler
from uuid import UUID

import yaml
from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from openapi_schema_validator import OAS30Validator
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from . import store
from .store import ApiError, ROOT

log = logging.getLogger("loanflow")


def expand(doc, value):
    if isinstance(value, dict):
        if "$ref" in value:
            target = doc
            for part in value["$ref"][2:].split("/"):
                target = target[part]
            return expand(doc, target)
        return {key: expand(doc, item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand(doc, item) for item in value]
    return value


@lru_cache(maxsize=None)
def document(name):
    return yaml.safe_load((ROOT / f"api/{name}.openapi.yaml").read_text())


def validator(doc, schema):
    return OAS30Validator(expand(doc, schema), format_checker=OAS30Validator.FORMAT_CHECKER)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def bearer(request):
    scheme, _, value = request.headers.get("Authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not value:
        raise ApiError(401, "UNAUTHENTICATED")
    return value


@dataclass
class Context:
    db: object
    clock: object
    actor: dict
    body: dict
    identification: str | None = None


def routes(name, invoke, invalid, clock=store.now):
    doc = document(name)
    app = FastAPI(docs_url=None, redoc_url=None, redirect_slashes=False)
    app.openapi = lambda: doc

    @app.exception_handler(HTTPException)
    async def http_error(_request, exc):
        return JSONResponse({"code": "NOT_FOUND" if exc.status_code == 404 else "INVALID_REQUEST",
                             "message": "HTTP error"}, status_code=exc.status_code)

    def endpoint(path, method, operation):
        parameters = [expand(doc, param) for param in operation.get("parameters", [])]
        params = [(p, validator(doc, p["schema"])) for p in parameters]
        body_spec = operation.get("requestBody", {})
        body_validator = validator(doc, body_spec["content"]["application/json"]["schema"]) if body_spec else None

        async def handle(request: Request):
            args = {}
            key = None
            identification = None
            try:
                sources = {"path": request.path_params, "query": request.query_params, "header": request.headers}
                for param, check in params:
                    location, field = param["in"], param["name"]
                    value = sources[location].get(field)
                    if value is None and not param.get("required"):
                        continue
                    if value is not None and param["schema"].get("type") == "integer":
                        value = int(value)
                        if value > 2**63 - 1:
                            raise ValueError
                    if not check.is_valid(value):
                        raise ValueError
                    if param["schema"].get("format") == "uuid":
                        value = UUID(value)
                    if field == "Idempotency-Key":
                        key = value
                    elif field == "X-Identification-Token":
                        identification = value
                    else:
                        args[field] = value
                raw = await request.body()
                if raw:
                    if not body_validator or request.headers.get("content-type", "").split(";")[0] != "application/json":
                        raise ValueError
                    body = json.loads(raw)
                else:
                    if body_spec.get("required"):
                        raise ValueError
                    body = {}
                if body_validator and not body_validator.is_valid(body):
                    raise ValueError
            except (ValueError, TypeError, OverflowError, RecursionError, ClientDisconnect):
                return invalid(operation, args)
            try:
                status, payload = await run_in_threadpool(invoke, operation, request, method, path, args, key,
                                                          identification, body, clock)
                if payload is None:
                    return Response(status_code=status)
                if isinstance(payload, bytes):
                    return Response(payload, media_type="application/pdf", status_code=status)
                return JSONResponse(store.as_json(payload), status_code=status)
            except ApiError as exc:
                headers = {"WWW-Authenticate": "Bearer"} if exc.status == 401 else None
                return JSONResponse(exc.body, status_code=exc.status, headers=headers)
            except Exception as exc:
                log.error("operation_failed operation=%s category=%s sqlstate=%s",
                          operation["operationId"], type(exc).__name__, getattr(exc, "sqlstate", None))
                if name == "checks":
                    return Response(status_code=503)
                error = {"code": "SERVICE_UNAVAILABLE"}
                if name == "abs":
                    error["instruction_id"] = str(args["instruction_id"])
                else:
                    error["message"] = "SERVICE_UNAVAILABLE"
                return JSONResponse(error, status_code=503)

        return handle

    for path, methods in doc["paths"].items():
        for method, operation in methods.items():
            if method in {"get", "post", "patch", "put"}:
                app.add_api_route(path, endpoint(path, method, operation), methods=[method.upper()],
                                  name=operation["operationId"])
    return app
