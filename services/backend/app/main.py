import os

from app.api.v1.router import api_router as api_router_v1
from app.const import OPEN_API_DESCRIPTION, OPEN_API_TITLE
from app.middleware.health_check import health_check_middleware

# from app.services.auth import OldPermissionChecker
from app.version import __version__
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi_pagination import add_pagination

deploy_env = os.getenv("APPSETTING_DEPLOY_ENV", "local").lower()
prod = deploy_env == "prod"
docs_url = None if prod else "/docs"
openapi_url = None if prod else "/openapi.json"

app = FastAPI(
    title=OPEN_API_TITLE,
    description=OPEN_API_DESCRIPTION,
    version=__version__,
    swagger_ui_parameters={"defaultModelsExpandDepth": -1},
    docs_url=docs_url,
    redoc_url=None,
    openapi_url=openapi_url,
    # swagger_ui_oauth2_redirect_url="/auth/oauth2-redirect",
    # swagger_ui_init_oauth={
    #     "usePkceWithAuthorizationCodeGrant": True,
    #     "clientId": config.APPSETTING_MS_APP_CLIENT_ID,
    # },
)

origins = ["*"]


if prod:
    origins = [
        "localhost",
    ]

    # app.add_middleware(HTTPSRedirectMiddleware)
    @app.get("/openapi.json", include_in_schema=False)
    async def openapi(
        # user: Annotated[
        #     CurrUserSchema,
        #     # Depends(OldPermissionChecker(required_permissions=["docs:read"])),
        # ],
    ):
        return get_openapi(title=app.title, version=app.version, routes=app.routes)


# app.add_middleware(SessionMiddleware, secret_key=os.getenv("APPSETTING_TOKEN_KEY", False))
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
add_pagination(app)
app.middleware("http")(health_check_middleware)
app.include_router(api_router_v1, prefix="/v1")
