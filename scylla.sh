# Start a scylla instance with 4 shards
CONTAINER_TOOL="podman"

if ! [ -x "$(command -v podman)" ]; then
    CONTAINER_TOOL="docker"
fi

$CONTAINER_TOOL run --rm -it --name scylla -p 9042:9042 -p 19042:19042 scylladb/scylla --smp 4
