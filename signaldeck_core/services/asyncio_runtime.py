# signaldeck_core/services/asyncio_runtime.py
import asyncio
import threading
import logging
from concurrent.futures import ThreadPoolExecutor

class AsyncioRuntime:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self._loop = asyncio.new_event_loop()
        self._loop_ready = threading.Event()

        executor = ThreadPoolExecutor(max_workers=1)
        self._loop.set_default_executor(executor)

        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="AsyncioLoopThread",
        )
        self._thread.start()
        self._loop_ready.wait()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.call_soon(self._loop_ready.set)
        self._loop.run_forever()

    def schedule_coroutines(self, coros: list):
        for c in coros:
            self._loop.call_soon_threadsafe(self._loop.create_task, c)

    def shutdown_loop(self):
        tasks = [t for t in asyncio.all_tasks(self._loop) if not t.done()]
        for t in tasks:
            t.cancel()
        self.logger.info(f"Cancelled {len(tasks)} tasks")
        self._loop.call_soon_threadsafe(self._loop.stop)