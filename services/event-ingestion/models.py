from pydantic import BaseModel, Field
from datetime import datetime
from typing import Literal

# Defining the shape of the incoming event data
class EventPayload(BaseModel):
    user_id: str
    event_type: Literal['page_view', 'add_to_cart', 'checkout_start', 'click', 'purchase']
    payload: dict
    timestamp: datetime = Field(default_factory=datetime.now)
    
# Defining the shape of the API response
class EventResponse(BaseModel):
    event_id: str
    status: str