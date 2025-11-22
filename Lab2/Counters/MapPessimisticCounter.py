# import time
from Counters.BaseCounter import BaseCounter

class MapPessimisticCounter(BaseCounter):
    def __init__(self, client, name="map_pessimistic", key="counter"):
        super().__init__(client, name, key)  # HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        self.map = client.get_map(name).blocking()  # IMap<String, Value> map = hz.getMap( "map" );
        if self.map.get(key) is None:  #  String key = "1";
            self.map.put(key, 0)  # map.put( key, new Value() );

    def increment(self):
        self.map.lock(self.key)  # map.lock( key );
        try:  # try {
            v = self.map.get(self.key) or 0  # Value value = map.get( key );
            # time.sleep(1e-2)  # Thread.sleep( 10 );
            v += 1  # value.amount++;
            self.map.put(self.key, v)  # map.put( key, value );
        finally:  # } finally {
            self.map.unlock(self.key)  # map.unlock( key );
