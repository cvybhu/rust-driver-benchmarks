FROM ubuntu:18.04
RUN apt update

# Install required libraries
RUN apt install -y wget g++

# Install the C++ driver library
RUN wget https://github.com/scylladb/cpp-driver/releases/download/2.15.2-1/scylla-cpp-driver_2.15.2-1_amd64.deb
RUN wget https://github.com/scylladb/cpp-driver/releases/download/2.15.2-1/scylla-cpp-driver-dev_2.15.2-1_amd64.deb

RUN apt install -y ./scylla-cpp-driver_2.15.2-1_amd64.deb ./scylla-cpp-driver-dev_2.15.2-1_amd64.deb

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

# Compile the code
RUN g++ main.cpp -o basic -std=c++17 -lcassandra -O3 -flto -Wall -Wshadow