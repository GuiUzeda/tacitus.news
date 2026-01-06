import inspect

from fastapi.exceptions import HTTPException
from loguru import logger


def raise_with_log(status_code: int, detail: str) -> HTTPException:
    """Wrapper function for logging and raising exceptions."""

    desc = f"<HTTPException status_code={status_code} detail={detail}>"
    logger.error(f"{desc} | runner={runner_info()}")
    return HTTPException(status_code, detail)


def runner_info() -> str:
    info = inspect.getframeinfo(inspect.stack()[2][0])
    return f"{info.filename}:{info.function}:{info.lineno}"

# async def catch_exceptions_middleware(request: Request, call_next):
#     try:
#         return await call_next(request)
#     except SQLAlchemyError as e:
        
#         raise_with_log( detail= e.__str__(), status_code=403)