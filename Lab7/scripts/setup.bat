docker exec -it config-server mongosh --eval "rs.initiate({_id: 'rs_config', configsvr: true, members: [{ _id: 0, host: 'config-server:27017' }]})"

docker exec -it shard1 mongosh --eval "rs.initiate({_id: 'rs_shard1', members: [{ _id: 0, host: 'shard1:27017' }]})"
docker exec -it shard2 mongosh --eval "rs.initiate({_id: 'rs_shard2', members: [{ _id: 0, host: 'shard2:27017' }]})"
docker exec -it shard3 mongosh --eval "rs.initiate({_id: 'rs_shard3', members: [{ _id: 0, host: 'shard3:27017' }]})"

docker exec -it mongos mongosh -f "scripts/setup.js"