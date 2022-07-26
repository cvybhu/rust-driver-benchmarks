FROM ubuntu:18.04
RUN apt update

# Install java 8
RUN apt install -y openjdk-8-jdk
RUN apt install -y maven
#RUN update-alternatives --set java /usr/lib/jvm/jdk1.8.0_version/bin/java
RUN export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_version

# Copy benchmark code into the container
COPY source /source
WORKDIR /source

# Compile the code
RUN mvn clean package
