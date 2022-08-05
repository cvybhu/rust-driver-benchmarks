FROM ubuntu:18.04
RUN apt update

RUN apt install -y curl g++

# Install the latest Rust version
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh
RUN chmod +x rustup.sh
RUN ./rustup.sh -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

# Compile the code
RUN cargo build --release
RUN cp target/release/basic .

# Cleanup to reduce image size
RUN cargo clean
