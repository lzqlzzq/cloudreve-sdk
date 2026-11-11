"""Bounded operation scheduling and cancellation ownership for the async SDK."""
import asyncio
from contextvars import Context, copy_context


async def complete(awaitable, *, propagate_cancel=True):
    """Drain owned work before propagating cancellation, including repeated cancel()."""
    task = asyncio.ensure_future(awaitable)
    cancelled = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancelled = error
        except BaseException:
            break
    try:
        return task.result()
    finally:
        if cancelled is not None and propagate_cancel:
            raise cancelled


class _Job:
    def __init__(self, factory):
        self.factory = factory
        self.context = copy_context()
        self.result = asyncio.get_running_loop().create_future()
        self.task = None


class OperationQueue:
    def __init__(self, concurrency):
        if type(concurrency) is not int or concurrency <= 0:
            raise ValueError('max_concurrency must be a positive integer')
        self.concurrency = concurrency
        self.closed = False
        self._queue = asyncio.Queue(maxsize=concurrency)
        self._workers = []
        self._running = set()
        self._putters = set()

    async def submit(self, factory):
        # Nested SDK calls belong to the current operation and consume no extra worker.
        if asyncio.current_task() in self._running:
            return await factory()
        if self.closed:
            raise RuntimeError('Cloudreve client is closed')
        if not self._workers:
            self._workers = [Context().run(asyncio.create_task, self._work()) for _ in range(self.concurrency)]
        job = _Job(factory)
        try:
            if self._queue.full():
                putter = asyncio.create_task(self._queue.put(job))
                self._putters.add(putter)
                try:
                    await putter
                finally:
                    self._putters.discard(putter)
            else:
                self._queue.put_nowait(job)
            return await asyncio.shield(job.result)
        except asyncio.CancelledError as cancelled:
            if job.task is None:
                job.result.cancel()
            else:
                job.task.cancel()
            try:
                await complete(job.result, propagate_cancel=False)
            except asyncio.CancelledError:
                raise
            except BaseException as operation_error:
                # Cancellation wins, but retain the operation's outcome and cleanup details.
                raise cancelled from operation_error
            raise cancelled

    async def _work(self):
        while not self.closed:
            try:
                job = await self._queue.get()
            except asyncio.CancelledError:
                return
            try:
                if job.result.done():
                    continue
                job.task = job.context.run(asyncio.create_task, self._execute(job))
                try:
                    await job.task
                except asyncio.CancelledError:
                    # Shutdown may cancel a job before its coroutine starts.
                    if not job.result.done():
                        job.result.cancel()
            finally:
                self._queue.task_done()
                job = None

    async def _execute(self, job):
        # Register inside execution: eager task factories may run before create_task returns.
        task = asyncio.current_task()
        self._running.add(task)
        try:
            try:
                result = await job.factory()
            except BaseException as error:
                if not job.result.done():
                    job.result.set_exception(error)
            else:
                if not job.result.done():
                    job.result.set_result(result)
        finally:
            self._running.discard(task)

    async def aclose(self):
        self.closed = True
        putters = list(self._putters)
        for task in putters:
            task.cancel()
        await asyncio.gather(*putters, return_exceptions=True)
        while not self._queue.empty():
            job = self._queue.get_nowait()
            job.result.cancel()
            self._queue.task_done()
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
