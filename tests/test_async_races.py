"""Seeded schedule perturbation around queue admission, refresh, failure and shutdown."""
import asyncio
import random
import unittest
from collections import Counter

import httpx
from cloudreve import AsyncCloudreveV4, CloudreveError


def fresh_token():
    return {'access_token': 'access', 'refresh_token': 'refresh', 'access_expires': '2099-01-01T00:00:00Z'}


class RaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_seeded_refresh_cancel_error_close_interleavings(self):
        for seed in range(40):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                client = AsyncCloudreveV4(trust_env=False, max_concurrency=1 + seed % 4)
                client._set_token(fresh_token())
                baseline = set(asyncio.all_tasks())
                calls = Counter()
                active = peak = refreshes = 0
                async def request(method, url, **kwargs):
                    nonlocal active, peak, refreshes
                    if url.endswith('/session/token'):
                        return {'token': fresh_token(), 'user': {}}
                    if url.endswith('/refresh'):
                        self.assertEqual(active, 0, seed)
                        refreshes += 1
                        await asyncio.sleep(.001)
                        if seed % 7 == 0:
                            raise httpx.ReadTimeout('refresh response lost')
                        return fresh_token()
                    calls[url] += 1
                    active += 1
                    peak = max(peak, active)
                    try:
                        for _ in range(rng.randrange(1, 5)):
                            await asyncio.sleep(0)
                        if url.startswith('/item/') and int(url.rsplit('/', 1)[-1]) % 13 == 0:
                            raise CloudreveError(40076, 'injected conflict')
                        return url
                    finally:
                        active -= 1
                client._request = request
                tasks = []
                try:
                    for index in range(80):
                        tasks.append(asyncio.create_task(client.request('POST' if index % 3 == 0 else 'GET', '/item/'+str(index))))
                        if rng.random() < .3:
                            await asyncio.sleep(0)
                        if index in (8, 30):
                            client._access_expires = 0
                        if rng.random() < .3:
                            rng.choice(tasks).cancel()
                    tasks += [asyncio.create_task(client.refresh_access_token()) for _ in range(5)]
                    if seed % 2:
                        closes = [asyncio.create_task(client.aclose()) for _ in range(3)]
                        await asyncio.sleep(0)
                        closes[0].cancel()
                        results = await asyncio.wait_for(asyncio.gather(*closes, return_exceptions=True), 3)
                        self.assertTrue(all(result is None or isinstance(result, asyncio.CancelledError) for result in results))
                    results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3)
                    allowed = (asyncio.CancelledError, CloudreveError, httpx.ReadTimeout, RuntimeError)
                    self.assertTrue(all(isinstance(result, (str, type(None))) or isinstance(result, allowed) for result in results))
                    self.assertTrue(all(count == 1 for count in calls.values()), (seed, calls))
                    self.assertLessEqual(peak, client._operations.concurrency)
                    self.assertEqual(active, 0)
                    self.assertEqual(client._active_requests, 0)
                    self.assertFalse(client._auth_pending)
                    if seed % 7 == 0:
                        self.assertLessEqual(refreshes, 1)
                    if seed % 2 == 0:
                        await client.login('test', 'secret')
                        self.assertEqual(await client.request('GET', '/recovered'), '/recovered')
                finally:
                    await client.aclose()
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0)
                self.assertFalse(client._operations._running)
                self.assertFalse(client._operations._putters)
                self.assertEqual(client._operations._queue.qsize(), 0)
                self.assertEqual(set(asyncio.all_tasks()), baseline)

    @unittest.skipUnless(hasattr(asyncio, 'eager_task_factory'), 'Python 3.12 eager task factory')
    async def test_eager_tasks_preserve_nested_single_worker_operation(self):
        loop = asyncio.get_running_loop()
        original_factory = loop.get_task_factory()
        async with AsyncCloudreveV4(trust_env=False, max_concurrency=1) as client:
            async def request(method, url, **kwargs):
                return {'size': 1}
            client._request = request
            loop.set_task_factory(asyncio.eager_task_factory)
            task = asyncio.create_task(client.get_info('/file'))
            try:
                # A timeout must close the queue first so a broken nested admission cannot hang the suite.
                done, _ = await asyncio.wait([task], timeout=.2)
                if not done:
                    await client.aclose()
                self.assertTrue(done, 'Nested operation deadlocked under eager task scheduling')
                self.assertEqual(await task, {'size': 1})
            finally:
                loop.set_task_factory(original_factory)
                await client.aclose()
                await asyncio.gather(task, return_exceptions=True)
