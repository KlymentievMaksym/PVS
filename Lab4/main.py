import time
import subprocess
from multiprocessing import Process, Pool
from pymongo import MongoClient, WriteConcern



# def worker(write_concern):
#     client = MongoClient(URI)
#     col = client[DB].get_collection(COL, write_concern=write_concern)
#     for _ in range(INCREMENTS):
#         col.find_one_and_update({"_id": DOC_ID}, {"$inc": {"likes": 1}})
#     client.close()


def run_test(write_concern, kill_primary=False, clients=10, iterations=10):
    URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=replicaset"
    client = MongoClient(URI)
    DB = "testdb"
    COL = "likes"
    DOC_ID = "post1"
    col = client[DB][COL]
    col.delete_many({})
    col.insert_one({"_id": DOC_ID, "likes": 0})

    def sh(cmd):
        subprocess.run(cmd, shell=True, check=False)

    def worker(write_concern):
        col = client[DB].get_collection(COL, write_concern=write_concern)
        for _ in range(iterations):
            col.find_one_and_update({"_id": DOC_ID}, {"$inc": {"likes": 1}})


    print(f"[TEST] writeConcern = {write_concern.document}")

    procs = []
    start = time.time()

    with Pool(clients) as p:
        p.map(worker, [write_concern]*clients)

    # for _ in range(clients):
    #     p = Process(target=worker, args=(write_concern,))
    #     p.start()
    #     procs.append(p)

    if kill_primary:
        time.sleep(2)
        print(f"[KILL] Stopping PRIMARY (mongo1)")
        sh("docker stop mongo1")
        time.sleep(8)
        sh("docker start mongo1")

    # for p in procs:
    #     p.join()

    elapsed = time.time() - start

    # client = MongoClient(URI)
    result = client[DB][COL].find_one({"_id": DOC_ID})
    client.close()

    print(f"[RESULTS] Time: {elapsed:.2f} sec | likes = {result['likes']}")


if __name__ == "__main__":
    run_test(WriteConcern(w=1))
    # run_test(WriteConcern(w="majority"))

    run_test(WriteConcern(w=1), kill_primary=True)
    # run_test(WriteConcern(w="majority"), kill_primary=True)