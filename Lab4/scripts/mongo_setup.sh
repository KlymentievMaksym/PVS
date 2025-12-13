#!/bin/bash
echo "Waiting for MongoDB..."
sleep 5

mongosh --host mongo1:27017 <<EOF
  var cfg = {
    "_id": "replicaset",
    "version": 1,
    "members": [
      {
        "_id": 0,
        "host": "mongo1:27017",
        "priority": 2,
        "votes": 1
      },
      {
        "_id": 1,
        "host": "mongo2:27017",
        "priority": 1,
        "votes": 1
      },
      {
        "_id": 2,
        "host": "mongo3:27017",
        "priority": 1,
        "votes": 1
      }
    ]
  };
  rs.initiate(cfg);
EOF

echo "[INFO] Waiting until mongo1 becomes PRIMARY..."
until mongosh --host mongo1:27017 --quiet --eval "rs.isMaster().ismaster" | grep true; do
  sleep 1
done

# Додатково чекаємо, щоб всі SECONDARY синхронізувалися
echo "[INFO] Waiting for all nodes to be SECONDARY/PRIMARY..."
until [ $(mongosh --host mongo1:27017 --quiet --eval "rs.status().members.filter(m => m.stateStr === 'PRIMARY' || m.stateStr === 'SECONDARY').length") -eq 3 ]; do
  sleep 1
done

echo "[INFO] Replica set is ready. mongo1 is PRIMARY and all nodes visible."
