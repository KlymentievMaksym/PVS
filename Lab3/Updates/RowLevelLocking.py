from Updates.BaseUpdate import BaseUpdate


class RowLevelLocking(BaseUpdate):
    def worker(self, thread_id):
        connection = self.utilities.new_connection
        cursor = connection.cursor()
        for i in range(self.increments):
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE", (self.user_id,))
            results = cursor.fetchone()
            v = results[0]
            v += 1
            cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (v, self.user_id))
            connection.commit()
        connection.close()
