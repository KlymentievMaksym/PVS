import threading
import time

import hazelcast

from Counters import AtomicLongCounter, MapNoLockCounter, MapOptimisticCounter, MapPessimisticCounter

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

    throughput = (threads * increments_per_thread) / elapsed

    if isinstance(counter, AtomicLongCounter):
        final = counter.atomic.get()
    else:
        final = client.get_map(counter.name).blocking().get(counter.key)

    return final, elapsed, throughput


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
    try:
        for name, cls in variants.items():
            print("[MAIN] Running:", name)
            val, elapsed, throughput = run_benchmark(cls, client, threads=threads, increments_per_thread=increments_per_thread)
            print(f"[Results] Received: {val}\n[Results] Expected: {increments_per_thread * threads}\n[Results] Time: {elapsed:.2f} sec\n[Results] Throughput: {throughput:.2f} requests/sec\n", "-"*60)
    finally:
        client.shutdown()
