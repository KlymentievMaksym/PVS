from threading import Lock
import sqlite3 as sql
import os

path_to_root = os.getcwd()
if "Lab1" not in path_to_root:
    path_to_root += "\\Lab1"
if "Data" not in path_to_root:
    path_to_root += "\\Data\\"


class Counter:
    def __init__(self):
        self._lock = Lock()

    def inc_count(self):
        raise NotImplementedError

    def get_count(self):
        raise NotImplementedError


class CounterMem(Counter):
    def __init__(self):
        super().__init__()
        self.value = 0
        # self._lock = Lock()

    def inc_count(self):
        with self._lock:
            self.value += 1
            return self.value

    def get_count(self):
        with self._lock:
            return self.value


class CounterDB(Counter):
    def __init__(self, db_path: str = path_to_root + "\\data.db"):
        super().__init__()
        self.db_path = db_path
        # self._lock = Lock()
        self.conn = sql.connect(db_path, check_same_thread=False)  # Keep connection alive
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS counters (
                id INTEGER PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        cur.execute("INSERT OR IGNORE INTO counters (id, value) VALUES (1, 0)")
        cur.execute("UPDATE counters SET value = 0")
        self.conn.commit()

    def inc_count(self):
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("UPDATE counters SET value = value + 1")
            self.conn.commit()
            cur.execute("SELECT value FROM counters")
            return cur.fetchone()[0]

    def get_count(self):
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT value FROM counters")
            return cur.fetchone()[0]