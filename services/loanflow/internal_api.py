"""Доверенное чтение состояния для процесса оценки."""
import hmac
import os
from uuid import UUID

from fastapi import Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from . import store


def attach(app, dsn):
    def read(application_id):
        with store.connect(dsn) as db:
            return store.one(db, """SELECT application_id,submitted_at,status FROM application
                                    WHERE application_id=%s AND submitted_at IS NOT NULL""", (application_id,))

    @app.get("/internal/applications/{application_id}/scoring-state", include_in_schema=False)
    async def state(application_id: str, request: Request):
        scheme, _, token = request.headers.get("Authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not token:
            return JSONResponse({"code": "UNAUTHENTICATED"}, status_code=401,
                                headers={"WWW-Authenticate": "Bearer"})
        expected = os.environ.get("LOANFLOW_ASSESSMENT_TOKEN")
        if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
            return JSONResponse({"code": "FORBIDDEN"}, status_code=403)
        try:
            identifier = UUID(application_id)
        except ValueError:
            return JSONResponse({"code": "NOT_FOUND"}, status_code=404)
        try:
            row = await run_in_threadpool(read, identifier)
            if row is None:
                return JSONResponse({"code": "NOT_FOUND"}, status_code=404)
            return JSONResponse(store.as_json(row))
        except Exception:
            return JSONResponse({"code": "SERVICE_UNAVAILABLE"}, status_code=503)
