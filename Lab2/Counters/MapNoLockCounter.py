# import time
from Counters.BaseCounter import BaseCounter


class MapNoLockCounter(BaseCounter):
    def __init__(self, client, name="map_no_lock", key="counter"):
        super().__init__(client, name, key)  # HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        self.map = client.get_map(name).blocking() # IMap<String, Value> map = hz.getMap( "map" );
        if self.map.get(key) is None:  # String key = "1";
            self.map.put(key, 0)  # map.put( key, new Value() );

    def increment(self):
        v = self.map.get(self.key) or 0  # Value value = map.get( key );
        # time.sleep(1e-2)  # Thread.sleep( 10 );
        v += 1  # value.amount++;
        self.map.put(self.key, v)  # map.put( key, value );

