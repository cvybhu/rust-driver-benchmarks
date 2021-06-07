# Start a scylla instance with 4 shards
docker run --rm -it --name scylla -p 9042:9042 -p 19042:19042 scylladb/scylla --smp 4