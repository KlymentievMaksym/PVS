import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from Updates.Utilities import Utilities


class BaseUpdate:
    def __init__(self, user_id: int, threads: int, increments: int, database_params: str, serializable: bool = False):
        self.user_id = user_id
        self.threads = threads
        self.increments = increments
        self.utilities = Utilities(database_params, serializable)
        self.utilities.ensure_table
        self.utilities.ensure_user

    def run_threads(self, target):
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
        self.utilities.reset_counter(self.user_id)
        # elapsed = self.run_threads(self.worker)
        self.run_threads(self.worker)
        final = self.utilities.read_counter(self.user_id)[0]
        return final