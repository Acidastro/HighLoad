"""Интеграционный тест WS — Alice слушает, Bob публикует."""
import asyncio
import json
import sys
import time

import httpx
import websockets


async def main():
    jwt_a = open("/tmp/jwt_a").read().strip()
    jwt_b = open("/tmp/jwt_b").read().strip()

    received = []
    ws_url = f"ws://localhost:8090/post/feed/posted?token={jwt_a}"

    async with websockets.connect(ws_url) as ws:
        print(f"[{time.time():.2f}] WS connected as Alice")

        async def reader():
            try:
                async for msg in ws:
                    payload = json.loads(msg)
                    received.append(payload)
                    print(f"[{time.time():.2f}] WS recv: {payload}")
            except websockets.ConnectionClosed:
                pass

        reader_task = asyncio.create_task(reader())
        await asyncio.sleep(0.5)

        # Bob публикует пост
        async with httpx.AsyncClient() as client:
            t0 = time.time()
            r = await client.post(
                "http://localhost:8090/post/create",
                headers={"Authorization": f"Bearer {jwt_b}"},
                json={"text": "Hello from Bob, real-time test"},
            )
            t1 = time.time()
            print(f"[{t1:.2f}] POST /post/create → {r.status_code} {r.json()}, elapsed={1000*(t1-t0):.1f}ms")

        # Ждём 3 секунды на доставку
        await asyncio.sleep(3.0)
        reader_task.cancel()

    # Итог
    post_news = [m for m in received if m.get("type") == "post.new"]
    pings = [m for m in received if m.get("type") == "ping"]
    print(f"\n=== ИТОГ ===")
    print(f"Получено сообщений всего: {len(received)}")
    print(f"  post.new: {len(post_news)}")
    print(f"  ping:     {len(pings)}")
    if post_news:
        print(f"  Первый post.new: {post_news[0]}")
        print("[PASS] Realtime доставка работает")
        sys.exit(0)
    else:
        print("[FAIL] post.new не пришло")
        sys.exit(1)


asyncio.run(main())
