FROM ubuntu:18.04
RUN apt update

# Install required libraries
RUN apt install -y wget g++

# Install the C++ driver library
RUN wget https://github.com/scylladb/cpp-driver/releases/download/2.15.2-1/scylla-cpp-driver_2.15.2-1_amd64.deb
RUN wget https://github.com/scylladb/cpp-driver/releases/download/2.15.2-1/scylla-cpp-driver-dev_2.15.2-1_amd64.deb

RUN apt install -y ./scylla-cpp-driver_2.15.2-1_amd64.deb ./scylla-cpp-driver-dev_2.15.2-1_amd64.deb

# Download boost
# boost 1.65 is available on ubuntu 18.04 but it doesn't have boost::asio::thread_pool
RUN wget https://boostorg.jfrog.io/artifactory/main/release/1.76.0/source/boost_1_76_0.tar.gz
RUN tar -xzf boost_1_76_0.tar.gz

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

# Compile the code
RUN g++ main.cpp -o basic -std=c++17 -I/boost_1_76_0 -lcassandra -lpthread -O3 -flto -Wall -Wshadow