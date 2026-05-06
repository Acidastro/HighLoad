"""Тест Lady Gaga effect: автор-celebrity → push-skip → WS не приходит, но виден через GET feed."""
import asyncio
import json
import sys
import time

import asyncpg
import httpx
import websockets


async def main():
    jwt_a = open("/tmp/jwt_a").read().strip()
    jwt_b = open("/tmp/jwt_b").read().strip()
    bob_id = "34216502-3b48-41a4-8cf4-11c5a0b5672e"

    # Делаем Боба celebrity: вписываем 10001 fake followers через прямой SQL
    # (порог = 10000)
    conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5432/social_network")
    try:
        # Добавим Боба в celebrity_cache в Redis сразу — самый простой способ
        # имитации, без денормализации БД.
        import redis.asyncio as aioredis
        r = aioredis.Redis.from_url("redis://localhost:6379/0")
        await r.set(f"celebrity:{bob_id}", "1", ex=300)
        await r.aclose()
        print(f"[setup] Боб помечен как celebrity в Redis")
    finally:
        await conn.close()

    received = []
    ws_url = f"ws://localhost:49883/post/feed/posted?token={jwt_a}"

    async with websockets.connect(ws_url) as ws:
        async def reader():
            try:
                async for msg in ws:
                    payload = json.loads(msg)
                    if payload.get("type") == "post.new":
                        received.append(payload)
                        print(f"[ws] получено post.new: {payload['post_id']}")
            except websockets.ConnectionClosed:
                pass

        rt = asyncio.create_task(reader())
        await asyncio.sleep(0.5)

        async with httpx.AsyncClient() as client:
            r = await client.post(
                "http://localhost:49885/post/create",
                headers={"Authorization": f"Bearer {jwt_b}"},
                json={"text": "Celebrity post — should NOT push to WS"},
            )
            post_id = r.json()["id"]
            print(f"[post] celebrity-Боб создал пост {post_id}")

            await asyncio.sleep(2.0)

            # Проверяем GET /post/feed — пост должен быть виден через pull
            r2 = await client.get(
                "http://localhost:49883/post/feed?offset=0&limit=10",
                headers={"Authorization": f"Bearer {jwt_a}"},
            )
            feed = r2.json()
            in_feed = any(p["id"] == post_id for p in feed)
            print(f"[feed] GET /post/feed → {len(feed)} постов, наш пост в выдаче: {in_feed}")

        await asyncio.sleep(0.5)
        rt.cancel()

    print(f"\n=== ИТОГ celebrity test ===")
    print(f"WS-уведомлений post.new: {len(received)}")
    print(f"Пост в /post/feed (pull-fallback): {in_feed}")
    if len(received) == 0 and in_feed:
        print("[PASS] Celebrity skip + pull-fallback работают")
        sys.exit(0)
    print("[FAIL]")
    sys.exit(1)


asyncio.run(main())
