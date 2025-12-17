import hazelcast
import time
import threading
import psycopg2



class Counter:
    def __init__(self, database_params, info: bool = False):
        if info:
            print("[INFO] Connecting to Hazelcast...")

        self.database_params = database_params
        self.info = info

        self.client = hazelcast.HazelcastClient(
            cluster_name="dev",
            cluster_members=["localhost:5701"]
        )
        self.counters = self.client.get_map("counters").blocking()
        self._clear_database()
        self.get_info()

    def _get_connection(self):
        return psycopg2.connect(**self.database_params)

    def _clear_database(self):
        if self.info:
            print("[INFO] Clearing Counters...")

        self.counters.clear()
        for i in range(1, 5):
            self.counters.put(i, 0)
        time.sleep(1) # Чекаємо завершення запису в БД

    def close(self):
        self.client.shutdown()

    def get_info(self):
        connection = self._get_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT id, val FROM counters ORDER BY id")
        data = cursor.fetchall()
        cursor.close()
        connection.close()

        if self.info:
            print(f"[INFO] Data: {data}")
        return data

    def client_task(self, counter_id, requests_count):
        for _ in range(requests_count):
            # Використовуємо блокування для атомарності (щоб не було lost updates)
            self.counters.lock(counter_id)
            try:
                val = self.counters.get(counter_id)
                self.counters.put(counter_id, val + 1)
            finally:
                self.counters.unlock(counter_id)

    def run(self, name, counter_id, clients, increments_per_client):
        if self.info:
            print(f"[INFO] Running {name}")
            print(f"[INFO] Counter ID: {counter_id}")
            print(f"[INFO] Clients: {clients} | Increments per client: {increments_per_client}")
        
        expected = clients * increments_per_client

        threads = []
        start_time = time.time()
        for _ in range(clients):
            t = threading.Thread(target=self.client_task, args=(counter_id, increments_per_client))
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()
            
        duration = time.time() - start_time
        throughput = expected / duration

        result = self.counters.get(counter_id)
        if self.info:
            print(f"[RESULT] Time: {duration:.4f} sec")
            print(f"[RESULT] Throughput: {throughput:.2f}")
            print(f"[RESULT] Received: {result:_} ({result/expected*100:.2f}% from Expected: {expected:_})")


if __name__ == "__main__":
    database_params = {
        "host": "localhost",
        "port": 5432,
        "dbname": "counter_db",
        "user": "postgres",
        "password": "postgres"
    }

    counter = Counter(database_params, info=True)
    counter.run("Counter 1", 1, 1, 10000)
    counter.run("Counter 2", 2, 2, 10000)
    counter.run("Counter 3", 3, 5, 10000)
    counter.run("Counter 4", 4, 10, 10000)
    # time.sleep(2) # Даємо трохи часу, якщо є черга запису (хоча у вас write-delay:0)
    counter.get_info()
    counter.close()
