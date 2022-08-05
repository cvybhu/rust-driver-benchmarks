set -e

for driver in cassandra-cpp cdrs-tokio datastax-cpp-driver gocql scylla-cpp-driver scylla-cpp-rust-driver scylla-rust-driver; do
    echo "Building $driver..."
    cd $driver
    ./build.sh
    cd ..
done

printf "\nDONE\n"
