import hazelcast
import time
import threading
import psycopg2

# Підключення до БД для перевірки (пункт 2 і 4)
def get_db_connection():
    return psycopg2.connect(
        host="localhost", database="counter_db", user="postgres", password="postgres"
    )

# Підключення до Hazelcast
hz = hazelcast.HazelcastClient(
    cluster_name="dev",
    cluster_members=["localhost:5701"]
)

# Отримуємо доступ до розподіленої мапи
# Завдяки конфігурації на сервері, ця мапа вже пов'язана з БД
counters_map = hz.get_map("counters").blocking()

print("Connected to Hazelcast via MapStore configuration.")
print("\n--- 2. Initialization ---")

# Очищуємо мапу (це викличе delete в БД)
counters_map.clear()

# Ініціалізуємо 4 каунтери
# Hazelcast автоматично викличе store() і запише це в PostgreSQL
for i in range(1, 5):
    counters_map.put(i, 0)
    print(f"Put key {i} = 0 to Hazelcast")

time.sleep(1) # Даємо час на транзакцію

# Перевіряємо прямим запитом до БД
conn = get_db_connection()
cur = conn.cursor()
cur.execute("SELECT * FROM counters ORDER BY id")
rows = cur.fetchall()
print("DB Content verified:", rows)
cur.close()
conn.close()

assert len(rows) == 4, "Database should have 4 rows"
print("\n--- 3. Performance Test ---")

NUM_INCREMENTS = 1000  # Зменшимо кількість для демо, бо запис у БД повільний
NUM_THREADS = 10

def increment_counter(counter_id):
    for _ in range(NUM_INCREMENTS):
        # Блокування важливе для коректності (Read-Modify-Write)
        counters_map.lock(counter_id)
        try:
            # READ-THROUGH (якщо в кеші пусто, завантажить з БД)
            val = counters_map.get(counter_id)
            val += 1
            # WRITE-THROUGH (запише в кеш, а кеш синхронно запише в БД)
            counters_map.put(counter_id, val)
        finally:
            counters_map.unlock(counter_id)

threads = []
start_time = time.time()

# Запускаємо потоки. 
# Thread 1 -> Counter 1
# Thread 2 -> Counter 2
# ...
# Thread 5 -> Counter 1 (щоб була конкуренція)
for i in range(NUM_THREADS):
    # Розподіляємо навантаження: id беремо як (i % 4) + 1
    c_id = (i % 4) + 1 
    t = threading.Thread(target=increment_counter, args=(c_id,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

end_time = time.time()
print(f"Time taken: {end_time - start_time:.4f} seconds")
print("\n--- 4. Verification ---")

# Очікувані значення
# 10 потоків, по 1000 інкрементів.
# Каунтерів 4. Загальна к-сть операцій: 10 * 1000 = 10,000.
# На кожен каунтер припадає: 10000 / 4 = 2500 інкрементів (при рівномірному розподілі)
# Але у циклі вище ми жорстко прив'язали:
# id=1 отримають потоки 0, 4, 8 (3 потоки * 1000 = 3000)
# id=2 отримають потоки 1, 5, 9 (3 потоки * 1000 = 3000)
# id=3 отримають потоки 2, 6    (2 потоки * 1000 = 2000)
# id=4 отримають потоки 3, 7    (2 потоки * 1000 = 2000)

expected_values = {
    1: 3000,
    2: 3000,
    3: 2000,
    4: 2000
}

# Перевірка в Hazelcast
for k, v in expected_values.items():
    actual = counters_map.get(k)
    print(f"Counter {k}: Expected {v}, Got in Cache {actual}")

# Перевірка в БД (найголовніше для Write-Through)
conn = get_db_connection()
cur = conn.cursor()
cur.execute("SELECT id, val FROM counters ORDER BY id")
db_rows = cur.fetchall()
print("\nFinal DB State:")
for row in db_rows:
    cid, val = row
    print(f"DB ID {cid}: {val}")
    if val != expected_values[cid]:
        print(f"❌ MISMATCH for ID {cid}")
    else:
        print(f"✅ MATCH for ID {cid}")

cur.close()
conn.close()
hz.shutdown()