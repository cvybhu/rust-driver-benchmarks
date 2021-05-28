FROM ubuntu:18.04
RUN apt update

# Install required libraries
RUN apt install -y curl wget g++ cargo libssl-dev libuv1-dev 

# Install DataStax C++ driver
RUN wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.16.0/cassandra-cpp-driver_2.16.0-1_amd64.deb
RUN wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.16.0/cassandra-cpp-driver-dev_2.16.0-1_amd64.deb

RUN apt install -y ./cassandra-cpp-driver_2.16.0-1_amd64.deb ./cassandra-cpp-driver-dev_2.16.0-1_amd64.deb

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

# Compile the code
RUN cargo build --release
RUN cp target/release/basic .

# Cleanup to reduce image size
RUN cargo clean
