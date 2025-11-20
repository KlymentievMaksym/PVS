from Updates.PgCounterBenchmark import PgCounterBenchmark
from Updates.Utilities import new_conn, reset_counter, read_counter, USER_ID

# ---------- 3) In-place update (atomic DB update) ----------
class InplaceUpdate(PgCounterBenchmark):
    def worker(self, thread_id):
        conn = new_conn(serializable=False)
        cur = conn.cursor()
        for i in range(self.incs):
            try:
                cur.execute("BEGIN")
                cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (USER_ID,))
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