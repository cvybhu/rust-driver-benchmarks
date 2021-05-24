FROM ubuntu:18.04
RUN apt update

# Install required libraries
RUN apt install -y curl g++

# cdrs-tokio requires the latest Rust version
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh
RUN chmod +x rustup.sh
RUN ./rustup.sh -y

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

# Compile the code
RUN /root/.cargo/bin/cargo build --release
RUN cp target/release/basic .

# Cleanup to reduce image size
RUN /root/.cargo/bin/cargo clean
