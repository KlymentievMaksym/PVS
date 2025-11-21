from Counters.BaseCounter import BaseCounter

class MapPessimisticCounter(BaseCounter):
    def __init__(self, client, name="map_pessimistic", key="counter"):
        super().__init__(client, name, key)
        self.map = client.get_map(name).blocking()
        if self.map.get(key) is None:
            self.map.put(key, 0)

    def increment(self):
        # блокування ключа на час операції
        self.map.lock(self.key)
        try:
            v = self.map.get(self.key) or 0
            self.map.put(self.key, v + 1)
        finally:
            self.map.unlock(self.key)

