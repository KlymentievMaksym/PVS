import time
import subprocess
from multiprocessing import Process, Pool
from pymongo import MongoClient, WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.errors import AutoReconnect, ConnectionFailure, WTimeoutError, ServerSelectionTimeoutError

# URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=replicaset"
URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=replicaset"


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
                print(get_topology_status())
        except WTimeoutError:
            print("[TIMEOUT] WTimeoutError")
        except ServerSelectionTimeoutError:
            print("[TIMEOUT] ServerSelectionTimeoutError")

def client_work(write_concern, timeout, read_concern=None):
    with MongoClient(URI, timeoutMS=timeout) as client:
        try:
            collection = client["testdb"].get_collection("likes", write_concern=write_concern, read_concern=read_concern)
            collection.find_one_and_update({"_id": "1"}, {"$inc": {"likes": 1}})
        except WTimeoutError:
            print("[TIMEOUT] WTimeoutError")
        except ServerSelectionTimeoutError:
            print("[TIMEOUT] ServerSelectionTimeoutError")

def receive_result(read_concern=None):
    with MongoClient(URI) as client:
        try:
            collection = client["testdb"].get_collection("likes", read_concern=read_concern)
            print(collection.find_one({"_id": "1"}))
        except WTimeoutError:
            print("[TIMEOUT] WTimeoutError")
        except ServerSelectionTimeoutError:
            print("[TIMEOUT] ServerSelectionTimeoutError")

def get_topology_status():
    with MongoClient(URI) as client:
        try:
            while True:
                try:
                    status = client.admin.command("hello") # або "isMaster" у старих версіях
                    return status.get("primary"), status.get("me")
                except (AutoReconnect, ConnectionFailure):
                    print("... Вибори нового Primary тривають ...")
                    time.sleep(1)
        except WTimeoutError:
            print("[TIMEOUT] WTimeoutError")
        except ServerSelectionTimeoutError:
            print("[TIMEOUT] ServerSelectionTimeoutError")

def task(welcome_text, node_to_stop, node_type, timeout=None, read_concern=None):
    print(f"[INFO] [{node_type}] {welcome_text}")
    processes = []
    sh(f"docker stop {node_to_stop}")
    for i in range(1):
        process = Process(target=client_work, args=(WriteConcern(w=3), timeout))
        process.start()
        processes.append(process)

    print(f"[INFO] [MID RESULT] Right after stoping {node_type}...")
    receive_result(read_concern)
    time.sleep(10)
    print(f"[INFO] [MID RESULT] After 10 seconds stoping {node_type}...")
    receive_result(read_concern)
    sh(f"docker start {node_to_stop}")

    for process in processes:
        process.join()

    print("[INFO] [FINAL RESULT]")
    receive_result(read_concern)

if __name__ == "__main__":
    preprocess_db(True)

    text = "Starting 1 client with w=3 write concern and no timeout..."
    task(text, "mongo1", "PRIMARY")
    task(text, "mongo3", "SECONDARY")
    text = "Starting 1 client with w=3 write concern and 'majority' read concern, but timeout = 3000..."
    # task(text, "mongo1", "PRIMARY")
    task(text, "mongo3", "SECONDARY", read_concern=ReadConcern("majority"), timeout=3000)

