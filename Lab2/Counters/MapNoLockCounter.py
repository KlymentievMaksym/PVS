from Counters.BaseCounter import BaseCounter


class MapNoLockCounter(BaseCounter):
    def __init__(self, client, name="map_no_lock", key="counter"):
        super().__init__(client, name, key)
        self.map = client.get_map(name).blocking()
        if self.map.get(key) is None:
            self.map.put(key, 0)

    def increment(self):
        v = self.map.get(self.key)
        # симуляція невеликої операційної затримки
        v = 0 if v is None else v
        new = v + 1
        self.map.put(self.key, new)
