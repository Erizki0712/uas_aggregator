from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import asyncio
import redis.asyncio as redis
import os
import json
import time
from sqlalchemy import select, func
from src.database import init_db, AsyncSessionLocal, EventLog
from src.schemas import EventCreate, StatsResponse
from src.consumer import consume_loop, BROKER_URL, QUEUE_NAME, STATS_KEY

START_TIME = time.time()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    task = asyncio.create_task(consume_loop())
    yield

app = FastAPI(lifespan=lifespan)
r_pool = redis.ConnectionPool.from_url(BROKER_URL, encoding="utf-8", decode_responses=True)

async def get_db_stats():
    async with AsyncSessionLocal() as session:
        result_unique = await session.execute(select(func.count(EventLog.id)))
        unique_count = result_unique.scalar()
        
        result_topics = await session.execute(
            select(EventLog.topic, func.count(EventLog.topic)).group_by(EventLog.topic)
        )
        topics = [{"topic": row[0], "count": row[1]} for row in result_topics.all()]
        return unique_count, topics

@app.post("/publish")
async def publish_event(event: EventCreate):
    try:
        r = redis.Redis(connection_pool=r_pool)
        data = event.model_dump(mode='json') 
        await r.lpush(QUEUE_NAME, json.dumps(data))
        return {"status": "queued", "event_id": event.event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/publish/batch")
async def publish_batch(events: list[EventCreate]):
    try:
        r = redis.Redis(connection_pool=r_pool)
        pipeline = r.pipeline()
        for event in events:
            data = event.model_dump(mode='json')
            pipeline.lpush(QUEUE_NAME, json.dumps(data))
        await pipeline.execute()
        return {"status": "batch_queued", "count": len(events)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events")
async def get_events(topic: str = None, limit: int = 100):
    async with AsyncSessionLocal() as session:
        query = select(EventLog).limit(limit).order_by(EventLog.timestamp.desc())
        if topic:
            query = query.where(EventLog.topic == topic)
        result = await session.execute(query)
        events = result.scalars().all()
        return events

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    r = redis.Redis(connection_pool=r_pool)
    received_str = await r.get(STATS_KEY)
    received = int(received_str) if received_str else 0
    unique, topics = await get_db_stats()
    
    return {
        "total_received_queued": received,
        "unique_processed_db": unique,
        "estimated_duplicate_dropped": received - unique,
        "topics_count": topics,
        "uptime_seconds": time.time() - START_TIME
    }