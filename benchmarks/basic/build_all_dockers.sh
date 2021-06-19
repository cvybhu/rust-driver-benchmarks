set -e

for driver in cassandra-cpp cdrs-tokio cpp cpp-multi gocql rust; do
    echo "Building $driver..."
    cd $driver
    ./build.sh
    cd ..
done

printf "\nDONE\n"
