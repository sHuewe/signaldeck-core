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
        async def _cancel_all():
            current = asyncio.current_task()
            tasks = [t for t in asyncio.all_tasks() if t is not current and not t.done()]

            self.logger.info(f"Cancelling {len(tasks)} asyncio tasks...")
            for t in tasks:
                t.cancel()

            # Warten, bis alle wirklich beendet sind
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # Optional: logge "harte" Exceptions (nicht CancelledError)
                for r in results:
                    if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                        self.logger.debug("Task ended with exception during shutdown", exc_info=r)

        try:
            fut = asyncio.run_coroutine_threadsafe(_cancel_all(), self._loop)
            fut.result(timeout=5.0)
        except Exception as e:
            # Timeout oder andere Fehler beim Shutdown: nicht hängen bleiben
            self.logger.warning(f"Asyncio shutdown had issues: {e!r}")

        # Loop stoppen
        self._loop.call_soon_threadsafe(self._loop.stop)