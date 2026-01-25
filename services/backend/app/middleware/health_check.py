from fastapi import Request
from fastapi.responses import PlainTextResponse


async def health_check_middleware(request: Request, call_next):
    if request.url.path == "/healthcheck":
        return PlainTextResponse("ok")
    return await call_next(request)
