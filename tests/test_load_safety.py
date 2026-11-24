"""Synthetic workload test for local backpressure limits.

This deliberately uses fake work and never calls Telegram, YouTube, AudD or FFmpeg.
It validates queue/semaphore bounds, not real-world capacity or end-to-end latency.
"""
import asyncio
import os
import unittest

os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN")

from main import (
    MAX_CONCURRENT_UPDATES,
    MAX_PENDING_UPDATES,
    MAX_PARALLEL_DL,
    MAX_PARALLEL_SEARCH,
    MAX_PARALLEL_SEND,
    MAX_PARALLEL_VIDEO,
)


class SyntheticLoadSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_thousand_synthetic_updates_stay_within_local_limits(self):
        queue = asyncio.Queue(maxsize=MAX_PENDING_UPDATES)
        search_gate = asyncio.Semaphore(MAX_PARALLEL_SEARCH)
        download_gate = asyncio.Semaphore(MAX_PARALLEL_DL)
        send_gate = asyncio.Semaphore(MAX_PARALLEL_SEND)
        video_gate = asyncio.Semaphore(MAX_PARALLEL_VIDEO)

        active = {
            "handlers": 0,
            "search": 0,
            "downloads": 0,
            "sends": 0,
            "video": 0,
        }
        peak = dict(active)
        processed = 0
        queue_peak = 0

        async def limited(gate, key, delay=0.0005):
            async with gate:
                active[key] += 1
                peak[key] = max(peak[key], active[key])
                try:
                    await asyncio.sleep(delay)
                finally:
                    active[key] -= 1

        async def handle(kind):
            nonlocal processed
            if kind == "search":
                await limited(search_gate, "search")
            elif kind == "download":
                async with download_gate:
                    active["downloads"] += 1
                    peak["downloads"] = max(peak["downloads"], active["downloads"])
                    try:
                        await asyncio.sleep(0.0005)
                        await limited(send_gate, "sends")
                    finally:
                        active["downloads"] -= 1
            elif kind == "video":
                async with video_gate:
                    active["video"] += 1
                    peak["video"] = max(peak["video"], active["video"])
                    try:
                        await asyncio.sleep(0.0005)
                        await limited(search_gate, "search")
                    finally:
                        active["video"] -= 1
            else:
                await limited(send_gate, "sends")
            processed += 1

        async def worker():
            while True:
                kind = await queue.get()
                active["handlers"] += 1
                peak["handlers"] = max(peak["handlers"], active["handlers"])
                try:
                    await handle(kind)
                finally:
                    active["handlers"] -= 1
                    queue.task_done()

        workers = [
            asyncio.create_task(worker())
            for _ in range(MAX_CONCURRENT_UPDATES)
        ]
        try:
            for index in range(1000):
                kind = ("search", "download", "video", "cached_send")[index % 4]
                await queue.put(kind)
                queue_peak = max(queue_peak, queue.qsize())
            await queue.join()
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        self.assertEqual(processed, 1000)
        self.assertEqual(queue_peak, MAX_PENDING_UPDATES)
        self.assertLessEqual(peak["handlers"], MAX_CONCURRENT_UPDATES)
        self.assertLessEqual(peak["search"], MAX_PARALLEL_SEARCH)
        self.assertLessEqual(peak["downloads"], MAX_PARALLEL_DL)
        self.assertLessEqual(peak["sends"], MAX_PARALLEL_SEND)
        self.assertLessEqual(peak["video"], MAX_PARALLEL_VIDEO)
        self.assertEqual(active, {key: 0 for key in active})


if __name__ == "__main__":
    unittest.main()
