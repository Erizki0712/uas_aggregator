import json
import asyncio
import logging
import redis.asyncio as redis
import os
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
from src.database import AsyncSessionLocal, EventLog

BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379/0")
QUEUE_NAME = "event_queue"
STATS_KEY = "stats:received_count"

logger = logging.getLogger("consumer")
logging.basicConfig(level=logging.INFO)

async def get_redis():
    return await redis.from_url(BROKER_URL, encoding="utf-8", decode_responses=True)

async def process_event(db, event_data: dict):
    # parsing isoformat to datetime
    ts_value = datetime.fromisoformat(event_data['timestamp'])
    
    if ts_value.tzinfo is not None:
        ts_value = ts_value.replace(tzinfo=None)
    
    stmt = insert(EventLog).values(
        topic=event_data['topic'],
        event_id=event_data['event_id'],
        timestamp=ts_value, 
        source=event_data['source'],
        payload=event_data['payload']
    ).on_conflict_do_nothing(
        index_elements=['topic', 'event_id']
    )
    result = await db.execute(stmt)
    return result.rowcount

async def consume_loop():
    r = await get_redis()
    logger.info("Consumer started. Waiting for events...")
    
    while True:
        try:
            item = await r.brpop(QUEUE_NAME, timeout=1)
            
            if item:
                _, raw_data = item
                event_data = json.loads(raw_data)
                
                await r.incr(STATS_KEY)

                async with AsyncSessionLocal() as session:
                    async with session.begin():
                        inserted = await process_event(session, event_data)
                        if inserted == 0:
                            logger.debug(f"Duplicate dropped: {event_data['event_id']}")
                        else:
                            logger.debug(f"Processed: {event_data['event_id']}")
                            
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
            await asyncio.sleep(1)