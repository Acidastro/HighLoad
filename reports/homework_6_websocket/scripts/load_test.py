"""Нагрузочный тест: 100 постов от Боба, измеряем latency POST → WS-доставка."""
import asyncio
import json
import statistics
import time

import httpx
import websockets


WS_PORT = 49883
POST_PORT = 49885
N_POSTS = 100


async def main():
    jwt_a = open("/tmp/jwt_a").read().strip()
    jwt_b = open("/tmp/jwt_b").read().strip()

    received: dict[str, float] = {}  # post_id -> ts
    ws_url = f"ws://localhost:{WS_PORT}/post/feed/posted?token={jwt_a}"

    async with websockets.connect(ws_url) as ws:
        async def reader():
            try:
                async for msg in ws:
                    payload = json.loads(msg)
                    if payload.get("type") == "post.new":
                        received[payload["post_id"]] = time.time()
            except websockets.ConnectionClosed:
                pass

        rt = asyncio.create_task(reader())
        await asyncio.sleep(0.3)

        sent: dict[str, float] = {}
        async with httpx.AsyncClient(timeout=30) as client:
            t_start = time.time()

            async def post_one(i: int):
                t0 = time.time()
                r = await client.post(
                    f"http://localhost:{POST_PORT}/post/create",
                    headers={"Authorization": f"Bearer {jwt_b}"},
                    json={"text": f"Load test #{i}"},
                )
                if r.status_code == 200:
                    pid = r.json()["id"]
                    sent[pid] = t0

            await asyncio.gather(*[post_one(i) for i in range(N_POSTS)])
            t_end = time.time()

        print(f"[load] {N_POSTS} POST'ов отправлено за {1000*(t_end-t_start):.1f}ms "
              f"({N_POSTS/(t_end-t_start):.1f} req/s)")

        # Ждём доставки
        for _ in range(50):
            if len(received) >= N_POSTS:
                break
            await asyncio.sleep(0.1)

        rt.cancel()

    delivered = [(pid, sent[pid], received[pid]) for pid in sent if pid in received]
    latencies_ms = [(rcv - snt) * 1000 for _, snt, rcv in delivered]

    print(f"\n=== НАГРУЗОЧНЫЕ МЕТРИКИ ===")
    print(f"Отправлено POST'ов:       {len(sent)}")
    print(f"Доставлено WS post.new:    {len(delivered)}")
    print(f"Потерь:                    {len(sent) - len(delivered)}")
    if latencies_ms:
        latencies_ms.sort()
        n = len(latencies_ms)
        print(f"Latency POST → WS-доставка:")
        print(f"  min  = {min(latencies_ms):.1f} ms")
        print(f"  p50  = {latencies_ms[n//2]:.1f} ms")
        print(f"  p95  = {latencies_ms[int(n*0.95)]:.1f} ms")
        print(f"  p99  = {latencies_ms[int(n*0.99)]:.1f} ms")
        print(f"  max  = {max(latencies_ms):.1f} ms")
        print(f"  avg  = {statistics.mean(latencies_ms):.1f} ms")


asyncio.run(main())
