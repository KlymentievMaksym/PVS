from Updates.PgCounterBenchmark import PgCounterBenchmark
from Updates.Utilities import new_conn, reset_counter, read_counter, USER_ID
import time


# ---------- 5) Optimistic concurrency control (version) ----------
class OptimisticConcurrency(PgCounterBenchmark):
    def worker(self, thread_id):
        conn = new_conn(serializable=False)
        cur = conn.cursor()
        for i in range(self.incs):
            while True:
                try:
                    cur.execute("BEGIN")
                    cur.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (USER_ID,))
                    row = cur.fetchone()
                    if not row:
                        cur.execute("INSERT INTO user_counter(user_id, counter, version) VALUES (%s, %s, %s)",
                                    (USER_ID, 1, 1))
                        conn.commit()
                        break
                    old_counter, old_ver = row
                    new_counter = old_counter + 1
                    new_ver = old_ver + 1
                    cur.execute(
                        "UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                        (new_counter, new_ver, USER_ID, old_ver)
                    )
                    conn.commit()
                    if cur.rowcount > 0:
                        break  # успішно оновили
                    # інакше — хтось інший оновив — повторити
                except Exception:
                    conn.rollback()
                    # невеликa пауза щоб уникнути гарячого циклу
                    time.sleep(0.0005)
        conn.close()

    def run(self):
        reset_counter()
        elapsed = self.run_threads(self.worker)
        final = read_counter()[0]
        return final, elapsed
