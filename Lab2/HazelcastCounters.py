import threading
import time
# from tqdm import tqdm
# import tqdm

import hazelcast

from Counters import AtomicLongCounter, MapNoLockCounter, MapOptimisticCounter, MapPessimisticCounter

def run_benchmark(counter_cls, client, threads: int = 10, increments_per_thread: int = 10_000):  # , log_every: int = 1000
    total = threads * increments_per_thread
    counter = counter_cls(client)
    if isinstance(counter, AtomicLongCounter):
        counter.atomic.set(0)
    else:
        m = client.get_map(counter.name).blocking()
        m.put(counter.key, 0)

    threads_list = []
    start = time.perf_counter()

    for t in range(threads):
        th = threading.Thread(target=counter.bulk_increment, args=(increments_per_thread, ))
        th.start()
        threads_list.append(th)

    for th in threads_list:
        th.join()

    elapsed = time.perf_counter() - start
    throughput = total / elapsed

    if isinstance(counter, AtomicLongCounter):
        final = counter.atomic.get()
    else:
        final = client.get_map(counter.name).blocking().get(counter.key)

    return final, total, elapsed, throughput


if __name__ == "__main__":
    print("[PRE] Connecting to Hazelcast...")
    client = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["hazelcast1:5701","hazelcast2:5702","hazelcast3:5703"]
    )


    print("[MAIN] Starting different Counters...")
    variants = {
        "Map no-lock": MapNoLockCounter,
        "Map pessimistic": MapPessimisticCounter,
        "Map optimistic": MapOptimisticCounter,
        "AtomicLong (CP)": AtomicLongCounter,
    }
    threads = 10
    increments_per_thread = 10_000
    # log_every = 100
    try:
        # for _ in range(5):
        for name, cls in variants.items():
            print("[MAIN] Running:", name)
            val, expected, elapsed, throughput = run_benchmark(cls, client, threads=threads, increments_per_thread=increments_per_thread)  # , log_every=log_every
            print(f"[Results] Received: {val}\n[Results] Expected: {expected}\n[Results] Time: {elapsed:.2f} sec\n[Results] Throughput: {throughput:.2f} requests/sec\n", "-"*60)
    finally:
        client.shutdown()
