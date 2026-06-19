import os
import json
import uuid
import logging
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI
from aiokafka import AIOKafkaProducer

from models import EventPayload, EventResponse

# Configuring a structure for logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Defining the logger for this service
logger = logging.getLogger("event-ingestion")

# Reading Kafka broker address from env variables
# Read Kafka bootstrap servers from env (backwards-compatible with common typos)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_BOOSTSTRAP_SERVERS") or "localhost:9092"

# Global producer space managed by the lifespan context manager
producer: AIOKafkaProducer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    It will start the Kafka producer when the app starts and close it when the app shuts down.
    """
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    await producer.start()
    logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
    yield
    await producer.stop()
    logger.info("Kafka producer stopped.")
    
app = FastAPI(title = "Event Ingestion Service", lifespan = lifespan)

@app.post('/events', response_model = EventResponse)
async def ingest_event(event: EventPayload):
    """
    Receives a user behavior event and publish it to Kafka.
    """
    event_id = str(uuid.uuid4())
    
    # Building the event message for Kafka
    event_data = {
        "event_id": event_id,
        "user_id": event.user_id,
        "event_type": event.event_type,
        "payload": event.payload,
        "created_at": event.timestamp.isoformat()
    }
    
    # Publishing to the user-events topic
    await producer.send_and_wait("user-events", value = event_data)
    
    logger.info("Event received: %s from user %s", event.event_type, event.user_id)
    
    return EventResponse(event_id = event_id, status = "accepted")

@app.get("/health")
async def health_check():
    """
    Simple health check endpoint.
    """
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

