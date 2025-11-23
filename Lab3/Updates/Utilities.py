import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_SERIALIZABLE


class Utilities:
    def __init__(self, database_params: dict, serializable: bool = False):
        self.database_params = database_params
        self.serializable = serializable

    @property
    def new_connection(self):
        connection = psycopg2.connect(**self.database_params)
        connection.autocommit = False
        if self.serializable:
            connection.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
        else:
            connection.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        return connection

    @property
    def ensure_table(self):
        with self.new_connection as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_counter (
                        user_id INTEGER PRIMARY KEY,
                        counter INTEGER NOT NULL DEFAULT 0,
                        version INTEGER NOT NULL DEFAULT 0
                    );
                """)
            connection.commit()

    # @property
    def ensure_user(self, user_id: int):
        with self.new_connection as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO user_counter(user_id, counter, version)
                    VALUES (%s, 0, 0)
                    ON CONFLICT (user_id) DO NOTHING
                """, (user_id,))
            connection.commit()

    # @property
    def reset_counter(self, user_id: int):
        connection = self.new_connection
        try:
            cursor = connection.cursor()
            cursor.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = %s", (user_id,))
            connection.commit()
        finally:
            connection.close()

    # @property
    def read_counter(self, user_id: int):
        connection = self.new_connection
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()
            return result if result else (0, 0)
        finally:
            connection.close()