"""Подключаемся WS, снимаем метрики брокера, отключаемся."""
import asyncio
import json
import subprocess

import httpx
import websockets


async def main():
    jwt_a = open("/tmp/jwt_a").read().strip()
    ws_url = "ws://localhost:49883/post/feed/posted?token=" + jwt_a

    async with websockets.connect(ws_url) as ws:
        await asyncio.sleep(0.5)
        # Снимок при активном коннекте
        async with httpx.AsyncClient(auth=("guest", "guest")) as client:
            r = await client.get("http://localhost:15673/api/queues")
            qs = r.json()
            print("=== QUEUES (с активным WS-коннектом Алисы) ===")
            for q in qs:
                print(f"  {q['name']:<35} type={q.get('type','classic'):<8} "
                      f"messages={q.get('messages',0):<4} consumers={q.get('consumers',0)}")

            r2 = await client.get(
                "http://localhost:15673/api/exchanges/%2F/posts.events/bindings/source"
            )
            print(f"\n=== BINDINGS posts.events ===")
            for b in r2.json():
                print(f"  rk='{b['routing_key']}' → queue '{b['destination']}'")

            r3 = await client.get("http://localhost:15673/api/overview")
            ov = r3.json()
            print(f"\n=== OVERVIEW ===")
            print(f"  connections: {ov['object_totals']['connections']}")
            print(f"  channels:    {ov['object_totals']['channels']}")
            print(f"  queues:      {ov['object_totals']['queues']}")
            print(f"  exchanges:   {ov['object_totals']['exchanges']}")
            ms = ov.get("message_stats", {})
            print(f"  publish_total: {ms.get('publish', 0)}")
            print(f"  deliver_total: {ms.get('deliver_get', 0)}")


asyncio.run(main())
