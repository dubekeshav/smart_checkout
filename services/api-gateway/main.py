import asyncio
import time
import uuid
import os
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.middleware.base import BaseHTTPMiddleware
import httpx

# Service URLs from environment variables
EVENT_INGESTION_URL = os.getenv("EVENT_INGESTION_URL", "http://event-ingestion:8001")
OFFER_AGENT_URL = os.getenv("OFFER_AGENT_URL", "http://offer-agent:8002")

# Rate Limiter: 10 requests per minute per client per IP
limiter = Limiter(key_func = get_remote_address)
app = FastAPI(title = "Smart Checkout API Gateway")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("api-gateway")

# Middleware that attachees a unique correlation ID to every request
class CorrelationIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        request.state.correlation_id = correlation_id
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response
    
app.add_middleware(CorrelationIDMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Request and Response Models
class PersonalizeRequest(BaseModel):
    user_id: str
    cart_value: float
    cart_categories: list[str]
    event_type: str = "checkout_start"
    
class PersonalizeResponse(BaseModel):
    user_id: str
    offer: dict
    reasoning: str
    correlation_id: str
    latency_ms: float
    
# Defining the personalize endpoint
@app.post("/personalize")
@limiter.limit("10/minute")
async def personalize(request: Request, body: PersonalizeRequest):
    """
    Orchestrates the full personalization flow.
    """
    start = time.time()
    correlation_id = request.state.correlation_id
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Sending the checkout event to the event-ingestion service
        await client.post(
            f"{EVENT_INGESTION_URL}/events",
            json = {
                "user_id": body.user_id,
                "event_type": body.event_type,
                "payload": {
                    "cart_value": body.cart_value,
                    "cart_categories": body.cart_categories,
                },
            },
        )
        
        # Brief pause to let the profiel builder consume the event
        await asyncio.sleep(0.5)
        
        # Asking the offer-agent for a personalization decision
        agent_response = await client.post(
            f"{OFFER_AGENT_URL}/decide",
            json = {
                "user_id": body.user_id,
                "cart_value": body.cart_value,
                "cart_categories": body.cart_categories
            }
        )

        if agent_response.status_code != 200:
            logger.error(
                "Offer agent returned non-200 status: %s body=%s",
                agent_response.status_code,
                agent_response.text,
            )
            raise HTTPException(
                status_code=502,
                detail="Offer agent request failed",
            )

        try:
            decision = agent_response.json()
        except ValueError:
            logger.error(
                "Unable to parse offer agent response as JSON: %s",
                agent_response.text,
            )
            raise HTTPException(
                status_code=502,
                detail="Invalid response from offer agent",
            )

        if not isinstance(decision, dict):
            logger.error("Offer agent returned unexpected payload type: %s", type(decision))
            raise HTTPException(
                status_code=502,
                detail="Unexpected response shape from offer agent",
            )

    latency_ms = (time.time() - start) * 1000
        
    return PersonalizeResponse(
        user_id = body.user_id,
        offer = {
            "offer_id": decision.get("offer_id"),
            "offer_name": decision.get("offer_name"),
            "discount_pct": decision.get("discount_pct")
        },
        reasoning = decision.get("reasoning", ""),
        correlation_id = correlation_id,
        latency_ms = round(latency_ms, 2)
    )
    
# Just some health check endpoints
@app.get("/health/liveness")
async def liveness():
    """
    Simple liveness probe - confirms the process is running.
    """
    return {"status": "alive"}

@app.get("/health/readiness")
async def readiness():
    """
    Readiness probe - checks that downstream services are reachable.
    """
    dependencies = {}
    async with httpx.AsyncClient() as client:
        # Check event-ingestion service
        try:
            r = await client.get(f"{EVENT_INGESTION_URL}/health", timeout=2.0)
            logger.info("Event Ingestion service check: status=%s body=%s", r.status_code, r.text)
            dependencies['event-ingestion'] = "healthy" if r.status_code == 200 else "unhealthy"
        except Exception as exc:
            logger.warning("Event Ingestion health check failed: %s", exc)
            dependencies["event-ingestion"] = "unhealthy"
        
        # Check offer-agent service
        try:
            r = await client.get(f"{OFFER_AGENT_URL}/health", timeout=2.0)
            logger.info("Offer Agent service check: status=%s body=%s", r.status_code, r.text)
            dependencies['offer-agent'] = "healthy" if r.status_code == 200 else "unhealthy"
        except Exception as exc:
            logger.warning("Offer Agent health check failed: %s", exc)
            dependencies["offer-agent"] = "unhealthy"
        
    all_healthy = all(v == "healthy" for v in dependencies.values())
    
    return {
        "status": "ready" if all_healthy else "degraded",
        "dependencies": dependencies
    }