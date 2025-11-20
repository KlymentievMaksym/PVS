# import threading
import time
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import psycopg2
# from psycopg2 import OperationalError, errors
# from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_SERIALIZABLE

from Updates import LostUpdate, OptimisticConcurrency, InplaceUpdate, RowLevelLocking, SerializableUpdate
from Updates import USER_ID, THREADS, INCREMENTS_PER_THREAD, EXPECTED, DB_CONFIG


# # ---------- Конфіг з'єднання (зміни під свій PG) ----------
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "testdb",
    "user": "postgres",
    "password": "postgres"
}

USER_ID = 1
THREADS = 10
INCREMENTS_PER_THREAD = 10_000
EXPECTED = THREADS * INCREMENTS_PER_THREAD


# ---------- CLI ----------
def main():
    variants = [
        ("Lost-update", LostUpdate()),
        ("Serializable update (with retry)", SerializableUpdate()),
        ("In-place UPDATE counter = counter + 1", InplaceUpdate()),
        ("Row-level locking (SELECT ... FOR UPDATE)", RowLevelLocking()),
        ("Optimistic concurrency (version + compare-and-set)", OptimisticConcurrency()),
    ]

    for name, instance in variants:
        print("Running variant:", name)
        start_total = time.perf_counter()
        final, elapsed = instance.run()
        total_time = time.perf_counter() - start_total
        print(f"  Final counter = {final}  (expected {EXPECTED})")
        print(f"  Variant time: {elapsed:.3f}s (wall clock for concurrent workers)")
        print(f"  Total elapsed (including setup): {total_time:.3f}s")
        print("-" * 60)

if __name__ == "__main__":
    main()
