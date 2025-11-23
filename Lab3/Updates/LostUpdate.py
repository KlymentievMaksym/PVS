from Updates.BaseUpdate import BaseUpdate


class LostUpdate(BaseUpdate):
    def worker(self, thread_id):
        connection = self.utilities.new_connection
        cursor = connection.cursor()
        for _ in range(self.increments):  # for (i in 1..10_000) {
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (self.user_id,))  # counter = cursor.execute(“SELECT counter FROM user_counter WHERE user_id = 1”).fetchone()
            results = cursor.fetchone()
            v = results[0]
            v += 1  # counter = counter + 1
            cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (v, self.user_id))  # cursor.execute(("update user_counter set counter = %s where user_id = %s", (counter, 1))
            connection.commit()  # conn.commit()
        connection.close()
