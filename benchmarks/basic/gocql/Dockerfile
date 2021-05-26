FROM golang

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

# Compile the code
RUN go build -o basic .
