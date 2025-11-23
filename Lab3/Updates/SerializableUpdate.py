from Updates.BaseUpdate import BaseUpdate
# import time

class SerializableUpdate(BaseUpdate):
    def worker(self, thread_id):
        connection = self.utilities.new_connection
        cursor = connection.cursor()
        for _ in range(self.increments):
            while True:
                try:
                    cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (self.user_id,))
                    results = cursor.fetchone()
                    v = results[0]
                    v += 1
                    cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (v, self.user_id))
                    connection.commit()
                    break
                except Exception as e:
                    connection.rollback()
                    # time.sleep(1e-3)
        connection.close()
