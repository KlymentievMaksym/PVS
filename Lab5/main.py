import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

def main():
    print("⏳ Підключення до кластера Cassandra (це може зайняти час)...")
    # Підключаємося до будь-якої ноди, драйвер сам знайде інші
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    print("✅ Підключено! Створюємо Keyspace (Базу даних)...")
    
    # Створюємо базу з реплікацією 3 (копія на кожній ноді)
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS lab5 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
    """)
    
    session.set_keyspace('lab5')

    # Створюємо таблицю
    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id int PRIMARY KEY,
            name text,
            role text
        )
    """)
    print("✅ Таблиця створена.")

    # ЗАПИС ДАНИХ (Consistency Level = ALL means all 3 nodes must confirm)
    print("📝 Записуємо дані з ConsistencyLevel.ALL...")
    query = SimpleStatement(
        "INSERT INTO users (id, name, role) VALUES (%s, %s, %s)",
        consistency_level=ConsistencyLevel.ALL
    )
    
    try:
        session.execute(query, (1, 'Ivan', 'Admin'))
        session.execute(query, (2, 'Petro', 'User'))
        print("✅ Дані успішно записані на всі 3 ноди.")
    except Exception as e:
        print(f"❌ Помилка запису: {e}")

    # ЧИТАННЯ ДАНИХ
    print("\n🔍 Читаємо дані...")
    rows = session.execute("SELECT * FROM users")
    for row in rows:
        print(f"   -> ID: {row.id}, Name: {row.name}, Role: {row.role}")

    print("\n--- ЕКСПЕРИМЕНТ ---")
    print("Тепер вручну зупини одну ноду (docker stop cas3) і спробуй прочитати знову!")
    
    # Безкінечний цикл читання, щоб ти встиг вбити ноду
    for i in range(20):
        try:
            print(f"Спроба читання {i+1}/20...", end=" ")
            # ONE означає: достатньо, щоб хоч одна жива нода відповіла
            read_query = SimpleStatement("SELECT * FROM users WHERE id=1", consistency_level=ConsistencyLevel.ONE)
            result = session.execute(read_query).one()
            print(f"OK! Name: {result.name}")
        except Exception as e:
            print(f"FAIL: {e}")
        time.sleep(2)

    cluster.shutdown()

if __name__ == "__main__":
    main()