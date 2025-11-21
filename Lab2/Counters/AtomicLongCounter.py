from Counters.BaseCounter import BaseCounter


class AtomicLongCounter(BaseCounter):
    def __init__(self, client, name="atomic_counter", key="counter"):
        super().__init__(client, name, key)
        self.atomic = client.cp_subsystem.get_atomic_long(name).blocking()
        # if self.atomic.get() is None: self.atomic.set(0)

    def increment(self):
        self.atomic.add_and_get(1)