"""One JSON error shape for every failure:

    {"error": {"code": "OBJECT_NOT_FOUND", "message": "Object ... does not exist"}}
"""
import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

log = logging.getLogger("controller.errors")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def not_found(object_id: str) -> ApiError:
    return ApiError(404, "OBJECT_NOT_FOUND", f"Object {object_id} does not exist")


def error_response(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


_HTTP_CODES = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError):
        return error_response(exc.status, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        # FastAPI answers 422 by default; the assignment's status list uses 400.
        problems = []
        for err in exc.errors():
            where = ".".join(str(p) for p in err.get("loc", []) if p not in ("body", "query", "path"))
            problems.append(f"{where}: {err.get('msg')}" if where else str(err.get("msg")))
        return error_response(400, "INVALID_REQUEST", "; ".join(problems) or "Invalid request")

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException):
        code = _HTTP_CODES.get(exc.status_code, "INVALID_REQUEST" if exc.status_code < 500 else "INTERNAL_ERROR")
        status = 400 if exc.status_code == 422 else exc.status_code
        return error_response(status, code, str(exc.detail))

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception):
        log.exception("unexpected error handling %s %s", request.method, request.url.path)
        return error_response(500, "INTERNAL_ERROR", "Internal server error")
