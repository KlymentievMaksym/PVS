from Updates.BaseUpdate import BaseUpdate


class InplaceUpdate(BaseUpdate):
    def worker(self, thread_id):
        connection = self.utilities.new_connection
        cursor = connection.cursor()
        for i in range(self.increments):
            cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (self.user_id,))
            connection.commit()
        connection.close()
