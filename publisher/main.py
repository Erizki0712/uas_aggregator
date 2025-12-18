import asyncio
import httpx
import os
import random
import uuid
from datetime import datetime, timezone

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "5000"))
BATCH_SIZE = 50

generated_ids = []

def generate_payload():
    return {
        "topic": random.choice(["order", "payment", "login", "sensor"]),
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "publisher-1",
        "payload": {"value": random.randint(1, 100)}
    }

async def sender():
    print(f"Starting publisher. Target: {TOTAL_EVENTS} events.")
    async with httpx.AsyncClient(timeout=30.0) as client:
        sent_count = 0
        while sent_count < TOTAL_EVENTS:
            batch = []
            for _ in range(BATCH_SIZE):
                if generated_ids and random.random() < 0.3:
                    dup_id = random.choice(generated_ids)
                    event = generate_payload()
                    event['event_id'] = dup_id 
                else:
                    event = generate_payload()
                    generated_ids.append(event['event_id'])
                    if len(generated_ids) > 5000:
                        generated_ids.pop(0)
                batch.append(event)

            try:
                resp = await client.post(f"{TARGET_URL}/batch", json=batch)
                if resp.status_code == 200:
                    sent_count += len(batch)
                    print(f"Sent: {sent_count}/{TOTAL_EVENTS}", end='\r')
                else:
                    print(f"Error: {resp.status_code}")
            except Exception as e:
                print(f"Connection error: {e}")
                await asyncio.sleep(2)
            await asyncio.sleep(0.01)

    print("\nPublisher finished.")

if __name__ == "__main__":
    asyncio.run(sender())