import asyncio
import json
import os
import logging

from typing import Dict

import asyncpg
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from features import compute_features

# Configuring loggin structure
logging.basicConfig(level = logging.INFO, format = "%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("profile-builder")

# Environment Configurations
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS') or 'localhost:9092'
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://checkout_user:checkout_pass@localhost:5432/checkout_db")
PROFILE_CACHE_TTL = 300 # 300 seconds of TTL for cache profiles

async def upsert_profile(db_conn, profile: Dict):
    '''
    Upsert a user computed profile into postres
    '''
    await db_conn.execute(
        """
        INSERT INTO user_profiles (user_id, purchase_frequency, avg_cart_value, category_affinity, recency_score, session_depth, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
            purchase_frequency = EXCLUDED.purchase_frequency,
            avg_cart_value = EXCLUDED.avg_cart_value,
            category_affinity = EXCLUDED.category_affinity,
            recency_score = EXCLUDED.recency_score,
            session_depth = EXCLUDED.session_depth,
            updated_at = NOW()
        """,
        profile["user_id"], profile['purchase_frequency'], profile['avg_cart_value'], json.dumps(profile['category_affinity']), profile['recency_score'], profile['session_depth']
    )
    
async def cache_profile(redis_client: redis.Redis, profile: Dict):
    '''
    Cache the computed profile in Redis with a TTL
    '''
    key = f"profile: {profile['user_id']}"
    await redis_client.set(key, ex = PROFILE_CACHE_TTL, value = json.dumps(profile))

async def batch_recompute(db_conn: asyncpg.Connection, redis_client: redis.Redis):
    '''
    Periodically recompute profiles for all users in the database to ensure data freshness.
    '''
    logger.info("Starting batch recomputation of all user profiles.")
    # Getting all unique user ids from the events table  
    rows = await db_conn.fetch("SELECT DISTINCT user_id FROM events")
    for row in rows:
        user_id = row['user_id']
        profile = await compute_features(user_id, {}, db_conn)
        await upsert_profile(db_conn, profile)
        await cache_profile(redis_client, profile)
        
        logger.info("Recomputed profile for user: {user_id}")
    logger.info("Batch recomputation complete. Processed {len(rows)} users.")

async def main():
    """
    Main entry point. Consumes events and build profiles.
    """
    logger.info("Profile Builder starting up...")
    
    # Connect to Postgres
    db_conn = await asyncpg.connect(DATABASE_URL)
    logger.info("Connected to Postgres database.")
    
    # Connect to Redis
    redis_client = redis.from_url(REDIS_URL)
    logger.info("Connected to Redis cache.")
    
    # Ensure the Kafka topic exists before starting the consumer
    admin = await asyncio.to_thread(KafkaAdminClient, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        try:
            await asyncio.to_thread(
                admin.create_topics,
                [NewTopic(name="user-events", num_partitions=3, replication_factor=1)],
                validate_only=False
            )
            logger.info("Created Kafka topic 'user-events'.")
        except TopicAlreadyExistsError:
            logger.info("Kafka topic 'user-events' already exists.")
    finally:
        await asyncio.to_thread(admin.close)

    await asyncio.sleep(2)

    # Creating Kafka consumer
    consumer = AIOKafkaConsumer(
        "user-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="profile-builder",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    await consumer.start()
    logger.info("Kafka consumer started on topic 'user-events'.")
    
    try:
        async for message in consumer:
            event = message.value
            user_id = event.get('user_id')
            event_type = event.get('event_type')
            logger.info("Processing event for user: %s, type: %s", user_id, event_type)
            
            # Computing updated features for this user
            profile = await compute_features(user_id, event, db_conn)
            
            # Upserting the computed profile into Postgres
            await upsert_profile(db_conn, profile)
            
            # Cache in Redis with TTL
            await cache_profile(redis_client, profile)
            
            logger.info(f"Profile updated for user: {user_id}")
            
    finally:
        await consumer.stop()
        await db_conn.close()
        await redis_client.close()
        logger.info("Profile Builder shutdown complete.")
        
if __name__ == "__main__":
    asyncio.run(main())