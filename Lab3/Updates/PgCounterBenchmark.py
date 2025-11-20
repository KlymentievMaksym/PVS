import time
from concurrent.futures import ThreadPoolExecutor, as_completed


THREADS = 10
INCREMENTS_PER_THREAD = 10_000

# ---------- Базовий клас для варіантів ----------
class PgCounterBenchmark:
    def __init__(self, threads=THREADS, incs=INCREMENTS_PER_THREAD):
        self.threads = threads
        self.incs = incs

    def run_threads(self, target):
        """
        target: function(thread_id) -> None, executed in each thread
        Each thread should create its own connection/cursor inside.
        """
        futures = []
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as ex:
            for t in range(self.threads):
                futures.append(ex.submit(target, t))
            # wait
            for f in as_completed(futures):
                exc = f.exception()
                if exc:
                    raise exc
        elapsed = time.perf_counter() - start
        return elapsed