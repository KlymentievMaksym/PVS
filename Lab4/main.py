import time
import subprocess
from multiprocessing import Process, Pool
from pymongo import MongoClient, WriteConcern

URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=replicaset"
DB = "testdb"
COL = "likes"
DOC_ID = "post1"


def prepare_db():
    with MongoClient(URI) as client:
        col = client[DB][COL]
        col.delete_many({})
        col.insert_one({"_id": DOC_ID, "likes": 0})

def worker(write_concern, iterations):
    with MongoClient(URI) as client:
        col = client[DB].get_collection(COL, write_concern=write_concern)
        for _ in range(iterations):
            col.find_one_and_update({"_id": DOC_ID}, {"$inc": {"likes": 1}})

def sh(cmd):
    print(cmd)
    subprocess.run(cmd, shell=True, check=False, capture_output=True)

def run_test(write_concern, kill_primary=False, clients=10, iterations=10):
    prepare_db()
    print(f"[TEST] writeConcern = {write_concern.document}")

    procs = []
    start = time.time()

    # print([[write_concern, iterations]]*clients)
    # with Pool(clients) as p:
    #     p.map(worker, [[write_concern, iterations]]*clients)

    for _ in range(clients):
        p = Process(target=worker, args=(write_concern, iterations))
        p.start()
        procs.append(p)

    print(client.primary)
    if kill_primary:
        time.sleep(2)
        primary = client.primary[0]
        print(f"[KILL] Stopping PRIMARY ({primary})")
        sh(f"docker stop {primary}")
        while client.primary is None:
            print(f"[WAIT] Waiting for new primary...")
            time.sleep(1)
        # time.sleep(6)
        sh(f"docker start {primary}")

    for p in procs:
        p.join()
    print(client.primary)

    elapsed = time.time() - start

    client = MongoClient(URI)
    result = client[DB][COL].find_one({"_id": DOC_ID})
    client.close()

    print(f"[RESULTS] Time: {elapsed:.2f} sec | likes = {result['likes']}")


if __name__ == "__main__":
    # run_test(WriteConcern(w=1))
    # run_test(WriteConcern(w="majority"))

    run_test(WriteConcern(w=1), kill_primary=True, iterations=10_00)
    run_test(WriteConcern(w="majority"), kill_primary=True, iterations=10_00)
