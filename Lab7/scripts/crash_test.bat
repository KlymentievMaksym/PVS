docker stop shard2
docker exec -it mongos mongosh -f "scripts/crash_test.js"
docker start shard2
docker exec -it mongos mongosh -f "scripts/crash_test.js"
