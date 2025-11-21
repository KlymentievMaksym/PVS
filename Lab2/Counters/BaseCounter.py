from abc import ABC, abstractmethod
import hazelcast


class BaseCounter(ABC):
    def __init__(self, client: hazelcast.HazelcastClient, name: str, key: str = "counter"):
        self.client = client
        self.name = name
        self.key = key

    @abstractmethod
    def increment(self):
        pass

    def bulk_increment(self, n: int):
        for _ in range(n):
            self.increment()
