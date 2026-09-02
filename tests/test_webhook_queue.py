from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from solana_roi.observation_store import ObservationEventStore
from solana_roi.webhook_queue import DurableHeliusWebhookQueue, HeliusWebhookWorker


class RecordingService:
    def __init__(self):
        self.calls = []

    async def ingest_webhook(self, payload, *, received_at=None):
        self.calls.append((payload, received_at))
        return []


class FlakyService:
    def __init__(self):
        self.calls = 0

    async def ingest_webhook(self, payload, *, received_at=None):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("transient collector failure")
        return []


async def _drain_once(worker: HeliusWebhookWorker, queue: DurableHeliusWebhookQueue):
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    for _ in range(100):
        if queue.status()["pending"] == 0:
            break
        await asyncio.sleep(.01)
    stop.set()
    await task


def test_webhook_payload_is_durable_idempotent_and_preserves_receipt_time(tmp_path):
    store = ObservationEventStore(tmp_path / "queue.sqlite3")
    queue = DurableHeliusWebhookQueue(store)
    received = datetime(2026, 9, 1, tzinfo=timezone.utc)
    payload = [{"signature": "abc", "type": "SWAP"}]

    first_id, inserted = queue.enqueue(payload, received_at=received)
    duplicate_id, duplicate_inserted = queue.enqueue(payload, received_at=received)
    assert inserted is True
    assert duplicate_inserted is False
    assert duplicate_id == first_id
    assert queue.status()["pending"] == 1

    service = RecordingService()
    asyncio.run(_drain_once(HeliusWebhookWorker(queue=queue, service=service), queue))
    assert queue.status()["pending"] == 0
    assert queue.status()["complete"] == 1
    assert service.calls == [(payload, received)]
    assert store.verify()


def test_webhook_worker_retries_without_dropping_durable_payload(tmp_path):
    store = ObservationEventStore(tmp_path / "retry.sqlite3")
    queue = DurableHeliusWebhookQueue(store)
    queue.enqueue({"signature": "retry"}, received_at=datetime(2026, 9, 1, tzinfo=timezone.utc))
    service = FlakyService()
    worker = HeliusWebhookWorker(queue=queue, service=service, error_sleep_seconds=.001)

    asyncio.run(_drain_once(worker, queue))
    status = queue.status()
    assert service.calls >= 2
    assert status["pending"] == 0
    assert status["complete"] == 1
    assert store.verify()
