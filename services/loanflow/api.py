import json
import re
from secrets import token_bytes

from fastapi.responses import JSONResponse
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from . import applications, contracts, offers, reconciliation, signing, store, underwriting
from .http import Context, bearer, document, routes
from .store import ApiError, as_json, digest, one


def create_app(dsn=None, clock=store.now):
    handlers = (applications, contracts, offers, reconciliation, signing, underwriting)

    contract = document("loanflow")
    registry = {}
    for methods in contract["paths"].values():
        for method, operation in methods.items():
            if method in {"get", "post", "patch", "put"}:
                operation_id = operation["operationId"]
                name = re.sub(r"(?<!^)(?=[A-Z])", "_", operation_id).lower()
                registry[operation_id] = next(getattr(module, name) for module in handlers if hasattr(module, name))

    def invoke(operation, request, method, path, args, key, identification, body, clock):
        handler = registry[operation["operationId"]]
        with store.connect(dsn) as db:
            session_query = "SELECT * FROM stand_session WHERE token_digest=%s"
            reading = method == "get"
            if reading:
                # Все запросы и расчёт доступных действий относятся к одному снимку чтения.
                db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            else:
                session_query += " FOR SHARE"
            actor = one(db, session_query, (digest("bearer", bearer(request)),))
            if not actor:
                raise ApiError(401, "UNAUTHENTICATED")
            if not actor["enabled"] or actor["role"] not in operation["x-roles"]:
                raise ApiError(403, "FORBIDDEN")
            if reading:
                read_at = clock()
                clock = lambda: read_at
            ctx = Context(db, clock, actor, body, identification)
            if key is None:
                return handler(ctx, **args)
            resource = path.format(**args)
            scope = (actor["principal"], actor["role"], method, resource, key)
            db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 301))", (str(scope),))
            fingerprint = digest("idempotency", body, identification,
                                 actor["session_ref"] if identification else None)
            existing = one(db, """SELECT * FROM accepted_request
                                  WHERE (principal,role,method,path,request_key)=(%s,%s,%s,%s,%s)""", scope)
            associated = str(scope).encode()
            cipher = AESGCM(digest("accepted-response-key"))
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise ApiError(409, "IDEMPOTENCY_KEY_REUSED")
                saved = bytes(existing["response_body"])
                result = json.loads(cipher.decrypt(saved[:12], saved[12:], associated))
                if actor["role"] == "CUSTOMER":
                    applications.own_application(ctx, args.get("application_id") or result["application_id"])
                return existing["response_status"], result
            status, result = handler(ctx, **args)
            nonce = token_bytes(12)
            encrypted = nonce + cipher.encrypt(nonce, json.dumps(as_json(result)).encode(), associated)
            db.execute("INSERT INTO accepted_request VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                       (*scope, fingerprint, status, encrypted))
            return status, result

    def invalid(_operation, _args):
        return JSONResponse({"code": "INVALID_REQUEST", "message": "INVALID_REQUEST"}, status_code=400)

    return routes("loanflow", invoke, invalid, clock)


app = create_app()
