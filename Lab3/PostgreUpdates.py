import time
from Updates import LostUpdate, OptimisticConcurrency, InplaceUpdate, RowLevelLocking, SerializableUpdate


if __name__ == "__main__":
    database_params = {
        "host": "database-lab3",
        "port": 5432,
        "dbname": "user_counter_db",
        "user": "postgres",
        "password": "postgres"
    }

    user_id = 1
    threads = 10
    increments_per_thread = 10_000
    expected = threads * increments_per_thread

    variants = {
        "Lost-update": LostUpdate,
        "Serializable update": SerializableUpdate,
        "In-place update": InplaceUpdate,
        "Row-level locking": RowLevelLocking,
        "Optimistic concurrency control": OptimisticConcurrency,
    }

    for name, variant in variants.items():
        print(f"[INFO] Running: {name}")
        # print(f"[INFO] Connecting to PostgreSQL...")
        var = variant(user_id, threads, increments_per_thread, database_params, serializable=(name == "Serializable update"))
        # print(f"[INFO] Starting time evaluation...")
        start_total = time.perf_counter()
        final = var.run()
        total_time = time.perf_counter() - start_total
        percent = (final / expected) * 100
        print(f"[RESULTS] Received: {final} ({percent:.2f}% from expected {expected})")
        print(f"[RESULTS] Elapsed: {total_time:.2f} sec")
        print("-" * 60)
