# import time
from Counters.BaseCounter import BaseCounter

class MapOptimisticCounter(BaseCounter):
    def __init__(self, client, name="map_optimistic", key="counter"):
        super().__init__(client, name, key)  # HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        self.map = client.get_map(name).blocking()  # IMap<String, Value> map = hz.getMap( "map" );
        if self.map.get(key) is None:  # String key = "1";
            self.map.put(key, 0)  # map.put( key, new Value() );

    def increment(self):
        while True:  # for (; ; ) {
            old = self.map.get(self.key) or 0  # Value oldValue = map.get( key );
            new = old + 1  # Value newValue = new Value( oldValue ); newValue.amount++;
            # time.sleep(1e-2)  # Thread.sleep( 10 );

            if self.map.replace_if_same(self.key, old, new):  # if ( map.replace( key, oldValue, newValue ) )
                break  # break;
