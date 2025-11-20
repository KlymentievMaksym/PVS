from Updates.PgCounterBenchmark import PgCounterBenchmark
from Updates.Utilities import new_conn, reset_counter, read_counter, USER_ID

# ---------- 1) Lost-update ----------
class LostUpdate(PgCounterBenchmark):
    def worker(self, thread_id):
        # кожен потік створює своє підключення
        conn = new_conn(serializable=False)
        cur = conn.cursor()
        for i in range(self.incs):
            # окрема транзакція на запис
            try:
                cur.execute("BEGIN")
                cur.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
                row = cur.fetchone()
                v = row[0] if row else 0
                v += 1
                cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (v, USER_ID))
                conn.commit()
            except Exception as e:
                conn.rollback()
                # тут ми просто ідемо далі
        conn.close()

    def run(self):
        reset_counter()
        elapsed = self.run_threads(self.worker)
        final = read_counter()[0]
        return final, elapsed