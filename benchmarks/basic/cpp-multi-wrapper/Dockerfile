FROM fedora

RUN dnf -y update
RUN dnf -y install cmake g++ git cargo clang

WORKDIR /wrapper
RUN git clone https://github.com/hackathon-rust-cpp/cpp-rust-driver.git
WORKDIR cpp-rust-driver
RUN cd scylla-rust-wrapper && cargo build --release

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

RUN cp /wrapper/cpp-rust-driver/scylla-rust-wrapper/target/release/libscylla*.a  /source
RUN cp /wrapper/cpp-rust-driver/include/*  /source

# Compile the code
 RUN g++ main.cpp -o basic -std=c++17 -I/boost_1_76_0 -L. -lscylla_cpp_driver -lpthread -O3 -flto -Wall -Wshadow -ldl
