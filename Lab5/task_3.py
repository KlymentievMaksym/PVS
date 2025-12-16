"""
Частина 3. Аналіз продуктивності та перевірка цілісності

Аналогічно попереднім завданням, необхідно, для кластеру налаштованому у попередній частині, створити таблицю з каунтером лайків.
Далі з 10 окремих клієнтів одночасно запустити інкерементацію каунтеру лайків по 10_000 на кожного клієнта з різними опціями взаємодії з Cassandra.

Таблиця має бути створена у Keyspace з replication factor 3.

Для створення каунтеру використовуйте спеціальний тип колонки - counter (цей тип буде підтримувати операції increment/decrement in-place):

1. Вказавши у параметрах запиту Consistency Level One (це буде означати, що запис відбувається синхронно тільки на одну ноду),
    запустіть 10 клієнтів з інкрементом по 10_000 на кожному з них.
    Виміряйте час виконання та перевірте чи кінцеве значення буде дорівнювати очікуваному - 100К
2. Вказавши у параметрах запиту Consistency Level QUORUM (це буде означати, що запис відбувається синхронно на більшість нод),
    запустіть 10 клієнтів з інкрементом по 10_000 на кожному з них.
    Виміряйте час виконання та перевірте чи кінцеве значення буде дорівнювати очікуваному - 100К
"""
import threading
import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

def run_benchmark(session, consistency_level, test_name, clients=10, increments_per_client=10000, id=1):
    target_likes = clients * increments_per_client
    print(f"[TEST] {test_name}")
    print("[DELETE] Deleting previous data...")
    session.execute(f"TRUNCATE shop_rf3.likes_counter")
    time.sleep(4)

    query = f"UPDATE shop_rf3.likes_counter SET likes = likes + 1 WHERE id = {id}"
    statement = SimpleStatement(query, consistency_level=consistency_level)

    def client_worker():
        for _ in range(increments_per_client):
            try:
                session.execute(statement)
            except Exception as e:
                print(f"Error: {e}")


    threads = []
    start_time = time.time()
    
    print(f"[START] Clients: {clients} | Increments per client: {increments_per_client}")
    for _ in range(clients):
        t = threading.Thread(target=client_worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    duration = end_time - start_time

    row = session.execute(f"SELECT likes FROM shop_rf3.likes_counter WHERE id = {id}").one()
    final_count = row.likes if row else 0

    print(f"[RESULTS] Duration: {duration:.2f} сек")
    print(f"[RESULTS] Throughput: {target_likes / duration:.0f} op/sec")
    print(f"[RESULTS] Expected: {target_likes:_}")
    print(f"[RESULTS] Received:  {final_count:_}")

if __name__ == "__main__":
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    try:
        run_benchmark(session, ConsistencyLevel.ONE, "CONSISTENCY ONE")
        run_benchmark(session, ConsistencyLevel.QUORUM, "CONSISTENCY QUORUM")
    finally:
        cluster.shutdown()