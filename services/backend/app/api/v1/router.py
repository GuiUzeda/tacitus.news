from fastapi import APIRouter
from app.api.v1.endpoints import events, articles

# This is the main router for V1
api_router = APIRouter()

# We attach the specific routes here
api_router.include_router(events.router, prefix="/events", tags=["Events"])
api_router.include_router(articles.router, prefix="/articles", tags=["Articles"])