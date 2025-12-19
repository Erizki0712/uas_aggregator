import pytest
import asyncio
import httpx
import uuid
import random
import time
from datetime import datetime, timezone
from sqlalchemy import text
from src.database import init_db, AsyncSessionLocal

BASE_URL = "http://localhost:8080"

async def force_clean_db():
    print("\n[*] Cleaning Database...")
    await init_db()
    async with AsyncSessionLocal() as session:
        # delete all data
        await session.execute(text("TRUNCATE TABLE event_logs RESTART IDENTITY CASCADE"))
        await session.commit()
    print("[*] Database Cleaned.")

@pytest.mark.asyncio
async def test_stress_20k_throughput():
    await force_clean_db()

    print("\n=== starting stress test ===")

    # config
    TOTAL_EVENTS = 20000
    DUP_RATE = 0.3
    BATCH_SIZE = 500
    
    # data generator
    print(f"[*] Generating payloads...")
    unique_count = int(TOTAL_EVENTS * (1 - DUP_RATE))
    duplicate_count = TOTAL_EVENTS - unique_count
    
    unique_ids = [str(uuid.uuid4()) for _ in range(unique_count)]
    dup_ids = [random.choice(unique_ids) for _ in range(duplicate_count)]
    all_event_ids = unique_ids + dup_ids
    random.shuffle(all_event_ids)
    
    assert len(all_event_ids) == TOTAL_EVENTS
    
    payloads = []
    for evt_id in all_event_ids:
        payloads.append({
            "topic": "stress-test",
            "event_id": evt_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest-stress",
            "payload": {"val": random.randint(1, 100)}
        })

    # exec
    print(f"[*] Sending {TOTAL_EVENTS} events via Batch API...")
    
    start_time = time.time()
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        for i in range(0, len(payloads), BATCH_SIZE):
            batch = payloads[i : i + BATCH_SIZE]
            resp = await client.post(f"{BASE_URL}/publish/batch", json=batch)
            assert resp.status_code == 200

    api_send_done_time = time.time()
    send_duration = api_send_done_time - start_time
    print(f"[*] API Send finished in {send_duration:.2f}s")
    print("[*] Waiting for Consumer to finish processing...")

    final_stats = {}
    
    async with httpx.AsyncClient() as client:
        while True:
            resp = await client.get(f"{BASE_URL}/stats")
            stats = resp.json()
            
            current_unique = stats['unique_processed_db']
            
            print(f"\r    Processing... DB Unik: {current_unique}/{unique_count}", end="")

            if current_unique >= unique_count:
                final_stats = stats
                break
            
            if time.time() - start_time > 60000:
                pytest.fail("timeout waiting for processing to finish")
                
            await asyncio.sleep(0.5)

    end_time = time.time()
    total_duration = end_time - start_time
    
    # report
    print("\n\n" + "="*40)
    print("       PERFORMANCE REPORT       ")
    print("="*40)
    print(f"Total Events Sent : {TOTAL_EVENTS}")
    print(f"Target Uniques    : {unique_count}")
    print(f"Target Duplicates : {duplicate_count}")
    print("-" * 40)
    print(f"Actual DB Uniques : {final_stats['unique_processed_db']}")
    print("-" * 40)
    print(f"Total Duration    : {total_duration:.4f} seconds")
    tps = TOTAL_EVENTS / total_duration
    print(f"Throughput (TPS)  : {tps:.2f} events/sec")
    print("="*40)

    assert final_stats['unique_processed_db'] == unique_count
    
    assert tps > 100, "Throughput too low"