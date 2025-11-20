import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_SERIALIZABLE

# ---------- Конфіг з'єднання (зміни під свій PG) ----------
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

# ---------- Утиліти ----------
def new_conn(serializable=False):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    if serializable:
        conn.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
    else:
        conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    return conn

def reset_counter():
    conn = new_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = %s", (USER_ID,))
        conn.commit()
    finally:
        conn.close()

def read_counter():
    conn = new_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (USER_ID,))
        r = cur.fetchone()
        return r
    finally:
        conn.close()