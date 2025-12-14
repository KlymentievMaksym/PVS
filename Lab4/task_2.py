import time
import subprocess
from multiprocessing import Process, Pool
from pymongo import MongoClient, WriteConcern
from pymongo.errors import NotPrimaryError, WriteConcernError, WTimeoutError, ServerSelectionTimeoutError, NetworkTimeout

# URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=replicaset"
URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=replicaset"

name_id = "1"

def sh(command: str):
    print(command)
    subprocess.run(command, shell=True)

def preprocess_db(info: bool = False):
    with MongoClient(URI) as client:
        try:
            client.admin.command("ping")
            database = client["testdb"]
            if database.get_collection("likes") is not None:
                database.drop_collection("likes")
            database.create_collection("likes")

            collection = database.get_collection("likes")
            collection.insert_one({"_id": "1", "likes": 0})

            if info:
                print(collection)
                print(client.primary)
                print(client.nodes)
        except WTimeoutError:
            print(f"[TIMEOUT] {preprocess_db.__name__} WTimeoutError")
        except ServerSelectionTimeoutError:
            print(f"[TIMEOUT] {preprocess_db.__name__} ServerSelectionTimeoutError")

def client_work(iterations, write_concern):
    with MongoClient(URI) as client:
        try:
            collection = client["testdb"].get_collection("likes", write_concern=write_concern)
            for _ in range(iterations):
                collection.find_one_and_update({"_id": "1"}, {"$inc": {"likes": 1}})
        except WTimeoutError:
            print(f"[TIMEOUT] {client_work.__name__} WTimeoutError")
        except ServerSelectionTimeoutError:
            print(f"[TIMEOUT] {client_work.__name__} ServerSelectionTimeoutError")
        except NetworkTimeout:
            print(f"[TIMEOUT] {client_work.__name__} NetworkTimeout")
        except WriteConcernError:
            print(f"[ERROR] {client_work.__name__} WriteConcernError")
        except NotPrimaryError:
            print(f"[ERROR] {client_work.__name__} NotPrimaryError...")
            # time.sleep(1)
            # client_work(iterations, write_concern)

def receive_result(read_concern=None):
    with MongoClient(URI) as client:
        try:
            collection = client["testdb"].get_collection("likes", read_concern=read_concern)
            print(collection.find_one({"_id": "1"}))
        except WTimeoutError:
            print(f"[TIMEOUT] {receive_result.__name__} WTimeoutError")
        except ServerSelectionTimeoutError:
            print(f"[TIMEOUT] {receive_result.__name__} ServerSelectionTimeoutError")

def task(iterations, clients, write_concern, kill_primary: bool = False):
    print(f"[INFO] Started task with writeConcern = {write_concern.document} and kill_primary = {kill_primary}")
    preprocess_db()
    print("[KILL] [START RESULT]")
    receive_result()

    processes = []
    start = time.time()
    for i in range(clients):
        process = Process(target=client_work, args=(iterations, write_concern))
        process.start()
        processes.append(process)

    if kill_primary:
        time.sleep(4)
        sh("docker stop mongo1")
        time.sleep(4)
        print("[KILL] [MID RESULT] After 4 seconds from stoping PRIMARY...")
        receive_result()
        sh("docker start mongo1")
        print("[KILL] [MID RESULT] After enabling OLD PRIMARY...")
        receive_result()

    for process in processes:
        process.join()
    elapsed = time.time() - start

    print("[KILL] [END RESULT]")
    receive_result()
    print(f"[RESULTS] Time: {elapsed:.2f} sec")

if __name__ == "__main__":
    w1 = WriteConcern(w=1)
    wm = WriteConcern(w="majority")

    clients = 10
    iterations = 10_000

    # task(iterations, clients, w1)
    # task(iterations, clients, wm)

    task(iterations, clients, w1, kill_primary=True)
    task(iterations, clients, wm, kill_primary=True)
