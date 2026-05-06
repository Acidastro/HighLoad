"""Sequential тест: 30 постов с интервалом 50мс — чистая end-to-end latency."""
import asyncio
import json
import statistics
import time

import httpx
import websockets


WS_PORT = 49883
POST_PORT = 49885
N = 30


async def main():
    jwt_a = open("/tmp/jwt_a").read().strip()
    jwt_b = open("/tmp/jwt_b").read().strip()

    received: dict[str, float] = {}
    sent: dict[str, float] = {}

    ws_url = f"ws://localhost:{WS_PORT}/post/feed/posted?token={jwt_a}"
    async with websockets.connect(ws_url) as ws:
        async def reader():
            try:
                async for msg in ws:
                    p = json.loads(msg)
                    if p.get("type") == "post.new":
                        received[p["post_id"]] = time.time()
            except websockets.ConnectionClosed:
                pass

        rt = asyncio.create_task(reader())
        await asyncio.sleep(0.3)

        async with httpx.AsyncClient(timeout=30) as client:
            for i in range(N):
                t0 = time.time()
                r = await client.post(
                    f"http://localhost:{POST_PORT}/post/create",
                    headers={"Authorization": f"Bearer {jwt_b}"},
                    json={"text": f"seq #{i}"},
                )
                pid = r.json()["id"]
                sent[pid] = t0
                await asyncio.sleep(0.05)

        # Ждём доставки
        for _ in range(50):
            if len(received) >= N:
                break
            await asyncio.sleep(0.1)
        rt.cancel()

    latencies = [(received[p] - sent[p]) * 1000 for p in sent if p in received]
    latencies.sort()
    n = len(latencies)
    print(f"Доставлено {n}/{N}")
    if latencies:
        print(f"min  = {min(latencies):.1f} ms")
        print(f"p50  = {latencies[n//2]:.1f} ms")
        print(f"p95  = {latencies[int(n*0.95)]:.1f} ms")
        print(f"p99  = {latencies[int(n*0.99)]:.1f} ms")
        print(f"max  = {max(latencies):.1f} ms")
        print(f"avg  = {statistics.mean(latencies):.1f} ms")


asyncio.run(main())
