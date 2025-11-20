from Updates.PgCounterBenchmark import PgCounterBenchmark
from Updates.Utilities import new_conn, reset_counter, read_counter, USER_ID

# ---------- 4) Row-level locking (SELECT ... FOR UPDATE) ----------
class RowLevelLocking(PgCounterBenchmark):
    def worker(self, thread_id):
        conn = new_conn(serializable=False)
        cur = conn.cursor()
        for i in range(self.incs):
            try:
                cur.execute("BEGIN")
                cur.execute("SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE", (USER_ID,))
                row = cur.fetchone()
                v = row[0] if row else 0
                v += 1
                cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (v, USER_ID))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        conn.close()

    def run(self):
        reset_counter()
        elapsed = self.run_threads(self.worker)
        final = read_counter()[0]
        return final, elapsed
