from Updates.BaseUpdate import BaseUpdate
# import time


class OptimisticConcurrency(BaseUpdate):
    def worker(self, thread_id):
        connection = self.utilities.new_connection
        cursor = connection.cursor()
        for i in range(self.increments):  # for (i in 1..10_000) {
            while True:  # while (True) {
                cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (self.user_id,))  # (counter, version) = cursor.execute(“SELECT counter, version FROM user_counter WHERE user_id = 1”).fetchone()
                results = cursor.fetchone()
                counter, old_version = results
                counter += 1
                new_version = old_version + 1
                cursor.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s", (counter, new_version, self.user_id, old_version))
                connection.commit()
                if cursor.rowcount > 0:  # count = cursor.rowcount 
                    break  # if (count > 0) break
        connection.close()

