import time
import subprocess
from multiprocessing import Process, Pool
from pymongo import MongoClient, WriteConcern
from pymongo.errors import AutoReconnect, ConnectionFailure, WTimeoutError, ServerSelectionTimeoutError, NetworkTimeout

URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=replicaset"
# URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=replicaset"

w1 = WriteConcern(w=1)
wm = WriteConcern(w="majority")
name_id = "1"

clients = 10
iterations = 10

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
            print("[TIMEOUT] WTimeoutError")
        except ServerSelectionTimeoutError:
            print("[TIMEOUT] ServerSelectionTimeoutError")
            time.sleep(1)
            preprocess_db(info)

def client_work(write_concern, timeout, read_concern=None):
    with MongoClient(URI, timeoutMS=timeout) as client:
        try:
            collection = client["testdb"].get_collection("likes", write_concern=write_concern, read_concern=read_concern)
            collection.find_one_and_update({"_id": "1"}, {"$inc": {"likes": 1}})
        except WTimeoutError:
            print("[TIMEOUT] WTimeoutError")
        except ServerSelectionTimeoutError:
            print("[TIMEOUT] ServerSelectionTimeoutError")
            time.sleep(1)
            client_work(write_concern, timeout, read_concern)
        except NetworkTimeout:
            print("[TIMEOUT] NetworkTimeout")

def receive_result(read_concern=None):
    with MongoClient(URI) as client:
        try:
            collection = client["testdb"].get_collection("likes", read_concern=read_concern)
            print(collection.find_one({"_id": "1"}))
        except WTimeoutError:
            print("[TIMEOUT] WTimeoutError")
        except ServerSelectionTimeoutError:
            print("[TIMEOUT] ServerSelectionTimeoutError")
            time.sleep(1)
            receive_result(read_concern)

if __name__ == "__main__":
    preprocess_db(True)

    processes = []
    for i in range(clients):
        process = Process(target=client_work, args=(iterations, w1))
        process.start()
        processes.append(process)

    time.sleep(1)
    sh("docker stop mongo1")
    with MongoClient(URI, serverMonitoringMode="poll") as client:
        for i in range(5):
            print(client.primary)
            # print(receive_result())
            time.sleep(1)
    sh("docker start mongo1")

    for process in processes:
        process.join()

    receive_result()


# URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=replicaset"
# DB = "testdb"
# COL = "likes"
# DOC_ID = "post1"


# def prepare_db():
#     client = MongoClient(URI)

#     print(client.primary)
#     print(client.nodes)

#     col = client[DB][COL]
#     col.delete_many({})
#     col.insert_one({"_id": DOC_ID, "likes": 0})
#     client.close()

# def worker(write_concern, iterations):
#     client = MongoClient(URI)
#     col = client[DB].get_collection(COL, write_concern=write_concern)
#     for _ in range(iterations):
#         col.find_one_and_update({"_id": DOC_ID}, {"$inc": {"likes": 1}})
#     client.close()

# def sh(cmd):
#     print(cmd)
#     subprocess.run(cmd, shell=True, check=False, capture_output=True)

# def run_test(write_concern, kill_primary=False, clients=10, iterations=10):
#     prepare_db()
#     print(f"[TEST] writeConcern = {write_concern.document}")

#     procs = []
#     start = time.time()

#     # print([[write_concern, iterations]]*clients)
#     # with Pool(clients) as p:
#     #     p.map(worker, [[write_concern, iterations]]*clients)

#     for _ in range(clients):
#         p = Process(target=worker, args=(write_concern, iterations))
#         p.start()
#         procs.append(p)

#     if kill_primary:
#         time.sleep(2)
#         # client = MongoClient(URI)
#         # primary = client.primary[0]
#         # print(f"[KILL] Stopping PRIMARY ({primary})")
#         # sh(f"docker stop {primary}")
#         # sh(f"docker stop mongo1")
#         # while client.primary is None:
#         #     print(f"[WAIT] Waiting for new primary...")
#         #     time.sleep(1)
#         # sh(f"docker start mongo1")
#         # sh(f"docker start {primary}")
#         # client.close()
#         # print(client.primary)

#     for p in procs:
#         p.join()

#     elapsed = time.time() - start

#     client = MongoClient(URI)
#     result = client[DB][COL].find_one({"_id": DOC_ID})
#     client.close()

#     print(f"[RESULTS] Time: {elapsed:.2f} sec | likes = {result['likes']}")


# if __name__ == "__main__":
#     iterations = 10_00
#     run_test(WriteConcern(w=1), iterations=10_00)
#     run_test(WriteConcern(w="majority"), iterations=10_00)

#     run_test(WriteConcern(w=1), kill_primary=True, iterations=10_00)
#     run_test(WriteConcern(w="majority"), kill_primary=True, iterations=10_00)
