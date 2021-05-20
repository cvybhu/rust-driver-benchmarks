# Start a scylla instance with 4 shards
docker run --rm -it --name scylla --hostname scylla scylladb/scylla --smp 4