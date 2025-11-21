from Counters.BaseCounter import BaseCounter


class AtomicLongCounter(BaseCounter):
    def __init__(self, client, name="atomic_counter", key="counter"):
        super().__init__(client, name, key)  # HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        self.atomic = client.cp_subsystem.get_atomic_long(name).blocking()  # IAtomicLong counter = hazelcastInstance.getCPSubsystem().getAtomicLong( "counter" );

    def increment(self):
        self.atomic.add_and_get(1)  # counter.incrementAndGet();
