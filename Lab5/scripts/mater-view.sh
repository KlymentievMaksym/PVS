# !/bin/bash
echo "materialized_views_enabled: true" >> /etc/cassandra/cassandra.yaml && docker-entrypoint.sh cassandra -f -R