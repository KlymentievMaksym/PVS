from Counters.BaseCounter import BaseCounter

class MapOptimisticCounter(BaseCounter):
    def __init__(self, client, name="map_optimistic", key="counter", max_retries=100):
        super().__init__(client, name, key)
        self.map = client.get_map(name).blocking()
        if self.map.get(key) is None:
            self.map.put(key, 0)
        self.max_retries = max_retries

    def increment(self):
        for _ in range(self.max_retries):
            old = self.map.get(self.key) or 0
            new = old + 1
            # replace returns True if successful (atomic compare-and-replace)
            ok = self.map.replace_if_same(self.key, old, new)
            if ok:
                return
        # якщо після max_retries не вдалося — як fallback зробимо блокування
        # (щоб не втрачати значення)
        # Простий fallback:
        self.map.lock(self.key)
        try:
            v = self.map.get(self.key) or 0
            self.map.put(self.key, v + 1)
        finally:
            self.map.unlock(self.key)

