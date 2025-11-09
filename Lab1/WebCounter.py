from fastapi import FastAPI
import uvicorn

from Counter import CounterMem, CounterDB


class WebCounterApp:
    def __init__(self, counter: str = 'mem'):
        wrong_name = NameError("Wrong Name. Expected one from ['mem', 'memory', 'db', 'database']")
        self.app = FastAPI(title="Web Counter")
        self.counter = CounterMem() if counter.lower() == 'mem' or counter.lower() == 'memory' else CounterDB() if counter.lower() == 'db' or counter.lower() == 'database' else wrong_name
        if self.counter is wrong_name:
            raise self.counter
        self._setup_routes()

    def _setup_routes(self):
        @self.app.get("/inc")
        def inc_count():
            value = self.counter.inc_count()
            return value

        @self.app.get("/cnt")
        def get_count():
            value = self.counter.get_count()
            return value

    def run(self, host: str = "localhost", port: int = 8000, log_level: str = None):
        uvicorn.run(self.app, host=host, port=port, log_level=log_level)


if __name__ == "__main__":
    web_app = WebCounterApp('db')
    web_app.run()
