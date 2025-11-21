from abc import ABC, abstractmethod
import hazelcast
from tqdm import tqdm


class BaseCounter(ABC):
    def __init__(self, client: hazelcast.HazelcastClient, name: str, key: str = "counter"):
        self.client = client
        self.name = name
        self.key = key

    @abstractmethod
    def increment(self):
        pass

    def bulk_increment(self, n: int):
        for _ in tqdm(range(n), desc=f"{self.name}: "):
            self.increment()
