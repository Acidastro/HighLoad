"""Тест масштабирования: Alice на одном инстансе, Bob публикует через другой."""
import asyncio
import json
import sys
import time

import httpx
import websockets


PORTS = [49883, 49885, 49884]


async def main():
    jwt_a = open("/tmp/jwt_a").read().strip()
    jwt_b = open("/tmp/jwt_b").read().strip()

    received_per_port = {p: [] for p in PORTS}
    tasks = []

    # Alice коннектится сразу ко ВСЕМ трём инстансам (в реальной жизни LB кинул бы один раз)
    # Имитация: эмулируем ситуацию, что Alice — только на инст-1
    alice_inst = PORTS[0]
    bob_post_inst = PORTS[2]  # Bob делает POST через инст-3

    ws_url = f"ws://localhost:{alice_inst}/post/feed/posted?token={jwt_a}"
    print(f"[scenario] Alice WS → инст {alice_inst}, Bob POST → инст {bob_post_inst}")

    async with websockets.connect(ws_url) as ws:
        async def reader():
            try:
                async for msg in ws:
                    payload = json.loads(msg)
                    received_per_port[alice_inst].append(payload)
                    print(f"[{time.time():.2f}] Alice (инст {alice_inst}) получил: {payload}")
            except websockets.ConnectionClosed:
                pass

        rt = asyncio.create_task(reader())
        await asyncio.sleep(0.5)

        async with httpx.AsyncClient() as client:
            t0 = time.time()
            r = await client.post(
                f"http://localhost:{bob_post_inst}/post/create",
                headers={"Authorization": f"Bearer {jwt_b}"},
                json={"text": f"Scale test from inst {bob_post_inst}"},
            )
            t1 = time.time()
            print(f"[{t1:.2f}] POST через инст {bob_post_inst} → {r.status_code}, elapsed={1000*(t1-t0):.1f}ms")

        await asyncio.sleep(3.0)
        rt.cancel()

    post_news = [m for m in received_per_port[alice_inst] if m.get("type") == "post.new"]
    print(f"\n=== ИТОГ scaling test ===")
    print(f"Alice получила post.new: {len(post_news)}")
    if post_news:
        print(f"[PASS] Cross-instance routing работает: POST через {bob_post_inst} → WS через {alice_inst}")
        sys.exit(0)
    print(f"[FAIL] post.new не пришло")
    sys.exit(1)


asyncio.run(main())
