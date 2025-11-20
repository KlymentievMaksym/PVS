from Counters.BaseCounter import BaseCounter

# =========================================================
# 4) IAtomicLong через CP Subsystem (найкоректніший)
# =========================================================
class AtomicLongCounter(BaseCounter):
    def __init__(self, client, name="atomic_counter", key="counter"):
        super().__init__(client, name, key)
        # Використовуємо CP Subsystem API (blocking proxy)
        # Заувага: cp_subsystem доступне у Python клієнті як client.cp_subsystem
        self.atomic = client.cp_subsystem.get_atomic_long(name).blocking()
        # Ініціалізація не потрібна, але можна:
        # if self.atomic.get() is None: self.atomic.set(0)

    def increment(self):
        # Використаємо atomic increment (atomic.add_and_get або increment_and_get)
        # Більшість реалізацій мають add_and_get
        self.atomic.add_and_get(1)