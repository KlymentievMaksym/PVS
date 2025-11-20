import threading
import time

import hazelcast
from tqdm import tqdm

from Counters import AtomicLongCounter, MapNoLockCounter, MapOptimisticCounter, MapPessimisticCounter


# =========================================================
# Бенчмарк функція
# =========================================================
def run_benchmark(counter_cls, client, threads=10, increments_per_thread=10_000):
    counter = counter_cls(client)
    # reset value to 0 where possible
    if isinstance(counter, AtomicLongCounter):
        counter.atomic.set(0)
    else:
        m = client.get_map(counter.name).blocking()
        m.put(counter.key, 0)

    threads_list = []
    start = time.perf_counter()
    for t in range(threads):
        th = threading.Thread(target=counter.bulk_increment, args=(increments_per_thread,))
        th.start()
        threads_list.append(th)
    for th in threads_list:
        th.join()
    elapsed = time.perf_counter() - start

    # прочитати фінальне значення
    if isinstance(counter, AtomicLongCounter):
        final = counter.atomic.get()
    else:
        final = client.get_map(counter.name).blocking().get(counter.key)
    return final, elapsed


# =========================================================
# Простий CLI для запуску
# =========================================================
if __name__ == "__main__":
    # Налаштуй client так, щоб він підключався до твоїх Hazelcast нод
    print("[PRE] Starting Hazelcast...")
    client = hazelcast.HazelcastClient(
        # тут можна вказати адреси членів (пример)
        cluster_name="dev",
        # cluster_members або cloud config за потреби
        # кластерні адреси наприклад: ["127.0.0.1:5701","127.0.0.1:5702","127.0.0.1:5703"]
        cluster_members=["127.0.0.1:5701","127.0.0.1:5702","127.0.0.1:5703"]
    )


    variants = {
        "Map no-lock": MapNoLockCounter,
        "Map pessimistic": MapPessimisticCounter,
        "Map optimistic": MapOptimisticCounter,
        "AtomicLong (CP)": AtomicLongCounter,
    }

    print("[MAIN] Starting different Counters...")
    try:
        for name, cls in tqdm(variants.items()):
            print("Running:", name)
            val, took = run_benchmark(cls, client, threads=10, increments_per_thread=10_000)
            print(f"  result: {val}  (expected: 100000)  time: {took:.3f}s\n")
    finally:
        client.shutdown()
