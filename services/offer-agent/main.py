import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from tools import init_tools
from agent import build_agent

# Request and response models
class DecideRequest(BaseModel):
    user_id: str
    cart_value: float
    cart_categories: list[str]
    
class DecideResponse(BaseModel):
    offer_id: str
    offer_name: str
    discount_pct: float
    reasoning: str

# Module-level agent reference
AGENT = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize connections and agent on startup.
    """
    global AGENT
    
    # Initialize tool dependencies
    database_url = os.environ.get("DATABASE_URL", "postgresql://checkout_user:checkout_pass@postgres:5432/checkout_db")
    
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    init_tools(database_url, redis_url)
    
    # Build the LangChain agent
    AGENT = build_agent(db_pool = None, redis_client = None)
    print("Offer Decision Agent ready")
    
    yield
    
    print("Shutting down offer decision agent")
    
app = FastAPI(title = "Decision Agent", lifespan = lifespan)

@app.post("/decide", response_model = DecideResponse)
async def decide(request: DecideRequest):
    """
    Ask the AI agent to select the best offer for a user.
    """
    # Build the prompt with user context
    prompt = (
        f"Select the best offer for user '{request.user_id}' "
        f"who has a cart worth ${request.cart_value:.2f} "
        f"with items in categories: {request.cart_categories}. "
        f"Return your answer as JSON with keys: "
        f"offer_id, offer_name, discount_pct, reasoning."
    )
    
    try:
        # Invoke the agent
        result = AGENT.invoke({
            "messages": [{"role": "user", "content": prompt}]
        })
        
        # Extract the final message content
        final_message = result["messages"][-1]
        content = final_message.content
        
        # Parse the JSON response from the agent
        if isinstance(content, str):
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                # If not pure JSON, return raw reasoning as fallback
                return DecideResponse(
                    offer_id = "fallback",
                    offer_name = "Default Offer",
                    discount_pct = 5.0,
                    reasoning = content
                )    
        else:
            data = {"reasoning": str(content)}
            
        return DecideResponse(
            offer_id = data.get("offer_id", "unknown"),
            offer_name = data.get("offer_namee", "Unknown Offer"),
            discount_pct = float(data.get("discount_pct", 0)),
            reasoning = data.get("reasoning", "No reasoning provided")
        )
        
    except Exception as e:
        raise HTTPException(status_code = 500, detail = str(e))
    
@app.get("/health")
async def health():
    """
    Health check endpoint.
    """
    return {"status": "ok"}