FROM ubuntu:18.04

RUN apt update
RUN DEBIAN_FRONTEND=noninteractive apt install -y cmake g++ git clang curl

# Building doesn't work with cargo installed via apt, requires a newer version I think
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh
RUN chmod +x rustup.sh
RUN ./rustup.sh -y

WORKDIR /wrapper
RUN git clone https://github.com/scylladb/cpp-rust-driver
WORKDIR cpp-rust-driver
RUN cd scylla-rust-wrapper && /root/.cargo/bin/cargo build --release

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

RUN cp /wrapper/cpp-rust-driver/scylla-rust-wrapper/target/release/libscylla*.a  /source
RUN cp /wrapper/cpp-rust-driver/include/*  /source

# Compile the code
RUN g++ main.cpp -o basic -std=c++17 -I/boost_1_76_0 -L. -lscylla_cpp_driver -lpthread -O3 -flto -Wall -Wshadow -ldl
