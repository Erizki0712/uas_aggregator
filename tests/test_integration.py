import pytest
import asyncio
import uuid
import httpx
from datetime import datetime, timezone
from sqlalchemy import select, text
from src.database import AsyncSessionLocal, EventLog, init_db

BASE_URL = "http://localhost:8080"

@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="module")
async def setup_db():
    await init_db()
    async with AsyncSessionLocal() as session:
        # clear table before test
        await session.execute(text("TRUNCATE TABLE event_logs RESTART IDENTITY CASCADE"))
        await session.commit()

@pytest.mark.asyncio
async def test_1_health_check(setup_db):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "uptime_seconds" in data

@pytest.mark.asyncio
async def test_2_publish_valid_event():
    """Validasi skema dan publish"""
    event = {
        "topic": "test-topic",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"foo": "bar"}
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BASE_URL}/publish", json=event)
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

@pytest.mark.asyncio
async def test_3_deduplication_logic():
    """kirim duplikat, hanya 1 diproses DB"""
    u_id = str(uuid.uuid4())
    event = {
        "topic": "dedup-test",
        "event_id": u_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {}
    }
    
    async with httpx.AsyncClient() as client:
        await client.post(f"{BASE_URL}/publish", json=event)
        await client.post(f"{BASE_URL}/publish", json=event)
        await client.post(f"{BASE_URL}/publish", json=event)
    
    await asyncio.sleep(2)
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(EventLog).where(EventLog.event_id == u_id)
        )
        assert len(result.all()) == 1

@pytest.mark.asyncio
async def test_4_stats_consistency():
    """get /stats konsisten"""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/stats")
        data = resp.json()
        assert data["unique_processed_db"] >= 0
        assert data["estimated_duplicate_dropped"] >= 0

@pytest.mark.asyncio
async def test_5_invalid_schema():
    """event tanpa field ditolak"""
    invalid_event = {"topic": "fail"} # missing event_id etc
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BASE_URL}/publish", json=invalid_event)
        assert resp.status_code == 422

@pytest.mark.asyncio
async def test_6_persistence_check():
    """persistent data]"""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(EventLog))
        assert len(result.all()) > 0

@pytest.mark.asyncio
async def test_7_get_events_filter():
    """get /events"""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/events?topic=dedup-test")
        data = resp.json()
        assert len(data) > 0
        assert data[0]["topic"] == "dedup-test"

@pytest.mark.asyncio
async def test_8_concurrency_simulation():
    """concurrency"""
    events = [{
        "topic": "race-test",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {}
    } for _ in range(50)]
    
    async with httpx.AsyncClient() as client:
        tasks = [client.post(f"{BASE_URL}/publish", json=e) for e in events]
        responses = await asyncio.gather(*tasks)
        
    assert all(r.status_code == 200 for r in responses)
    await asyncio.sleep(2) 

@pytest.mark.asyncio
async def test_9_batch_publish():
    """test batch endpoint"""
    batch = [{
        "topic": "batch",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {}
    } for _ in range(10)]
    
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BASE_URL}/publish/batch", json=batch)
        assert resp.status_code == 200
        assert resp.json()["count"] == 10

@pytest.mark.asyncio
async def test_10_database_connection():
    """db check"""
    async with AsyncSessionLocal() as session:
        try:
            await session.execute(text("SELECT 1"))
            connected = True
        except:
            connected = False
    assert connected is True

@pytest.mark.asyncio
async def test_11_timestamp_parsing():
    """db timestamp parse"""
    unique_id = str(uuid.uuid4())
    ts_str = "2025-01-01T12:00:00+00:00"
    event = {
        "topic": "time-test",
        "event_id": unique_id,
        "timestamp": ts_str,
        "source": "test",
        "payload": {}
    }
    async with httpx.AsyncClient() as client:
        await client.post(f"{BASE_URL}/publish", json=event)
    
    await asyncio.sleep(1)
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(EventLog).where(EventLog.event_id == unique_id))
        obj = res.scalar_one()
        assert obj.timestamp.year == 2025

@pytest.mark.asyncio
async def test_12_payload_integrity():
    """save json payload"""
    unique_id = str(uuid.uuid4())
    payload = {"nested": {"data": 123}, "list": [1,2]}
    event = {
        "topic": "payload-test",
        "event_id": unique_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": payload
    }
    async with httpx.AsyncClient() as client:
        await client.post(f"{BASE_URL}/publish", json=event)
        
    await asyncio.sleep(1)
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(EventLog).where(EventLog.event_id == unique_id))
        obj = res.scalar_one()
        assert obj.payload["nested"]["data"] == 123