import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from Updates.Utilities import Utilities


class BaseUpdate:
    def __init__(self, user_id: int, threads: int, increments: int, database_params: str, serializable: bool = False):
        self.user_id = user_id
        self.threads = threads
        self.increments = increments
        self.utilities = Utilities(user_id, database_params, serializable)
        self.utilities.ensure_table
        self.utilities.ensure_user

    def run_threads(self, target):
        """
        target: function(thread_id) -> None, executed in each thread
        Each thread should create its own connection/cursor inside.
        """
        futures = []
        # start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as ex:
            for t in range(self.threads):
                futures.append(ex.submit(target, t))
            for f in as_completed(futures):
                exc = f.exception()
                if exc:
                    raise exc
        # elapsed = time.perf_counter() - start
        # return elapsed

    # @property
    def run(self):
        self.utilities.reset_counter
        # elapsed = self.run_threads(self.worker)
        self.run_threads(self.worker)
        final = self.utilities.read_counter[0]
        return final