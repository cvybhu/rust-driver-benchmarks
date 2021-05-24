FROM ubuntu:18.04
RUN apt update

# Install required libraries
RUN apt install -y cargo

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

# Compile the code
RUN cargo build --release