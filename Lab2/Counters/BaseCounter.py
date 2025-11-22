from abc import ABC, abstractmethod
import hazelcast
# import time
# from tqdm import tqdm


class BaseCounter(ABC):
    def __init__(self, client: hazelcast.HazelcastClient, name: str, key: str = "counter"):
        self.client = client
        self.name = name
        self.key = key

    @abstractmethod
    def increment(self):
        pass

    def bulk_increment(self, n: int):  # , log_every: int = 1

        # for _ in tqdm(range(n), desc=f"{self.name}: "):
        for iteration in range(n):
            # t0 = time.time()
            self.increment()
            # dt = time.time() - t0
            # avg = dt if avg is None else avg * 0.95 + dt * 0.05

            # if iteration % log_every == 0:
            #     eta = avg * (n - iteration)
            #     print(f"[{iteration}/{n}] [{self.name}] ETA: {eta:.2f}s", flush=True)

