import time

import multiprocessing
import asyncio

from WebCounter import WebCounterApp
from Client import HttpClient

def run_server(counter_type, port):
    app = WebCounterApp(counter_type)
    app.run(port=port, log_level="critical")

if __name__ == "__main__":
    clients_amounts = [1, 2, 5, 10]
    requests_amount = 10_000


    for clients_amount in clients_amounts:
        total_requests = requests_amount * clients_amount
        # -------------------------------------------------- #

        p1 = multiprocessing.Process(target=run_server, args=('mem', 8000))
        p1.start()

        time.sleep(1)

        client = HttpClient("http://localhost:8000", concurrency=clients_amount)
        asyncio.run(client.run_load_test("inc", total_requests=total_requests, name="Memory"))

        p1.terminate()
        p1.join()

        # -------------------------------------------------- #

        p2 = multiprocessing.Process(target=run_server, args=('db', 8000))
        p2.start()

        time.sleep(1)


        client = HttpClient("http://localhost:8000", concurrency=clients_amount)
        asyncio.run(client.run_load_test("inc", total_requests=total_requests, name="DataBase"))

        p2.terminate()
        p2.join()
