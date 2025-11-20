from Updates.PgCounterBenchmark import PgCounterBenchmark
from Updates.Utilities import psycopg2, new_conn, reset_counter, read_counter, USER_ID
import time

# ---------- 2) Serializable update (з retry) ----------
class SerializableUpdate(PgCounterBenchmark):
    def worker(self, thread_id):
        conn = new_conn(serializable=True)
        cur = conn.cursor()
        for i in range(self.incs):
            # retry loop у разі помилки серіалізації
            attempt = 0
            while True:
                attempt += 1
                try:
                    cur.execute("BEGIN")
                    cur.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
                    row = cur.fetchone()
                    v = row[0] if row else 0
                    v += 1
                    cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (v, USER_ID))
                    conn.commit()
                    break
                except psycopg2.errors.SerializationFailure as se:
                    conn.rollback()
                    # коротка пауза (експоненційно, але тут спрощено)
                    time.sleep(0.001)
                    # повторити
                except Exception as e:
                    conn.rollback()
                    # інші помилки — підняти
                    raise
        conn.close()

    def run(self):
        reset_counter()
        elapsed = self.run_threads(self.worker)
        final = read_counter()[0]
        return final, elapsed
