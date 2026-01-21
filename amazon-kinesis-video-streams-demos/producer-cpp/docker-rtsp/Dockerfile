# ===================== Builder =====================
FROM ubuntu:20.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    byacc \
    cmake \
    curl \
    g++ \
    git \
    make \
    ninja-build \
    gstreamer1.0-plugins-base-apps \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    m4 \
    openjdk-8-jdk \
    pkg-config \
    xz-utils \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

# Shallow clone for speed
RUN git clone --depth 1 \
    https://github.com/awslabs/amazon-kinesis-video-streams-producer-sdk-cpp.git

WORKDIR /opt/amazon-kinesis-video-streams-producer-sdk-cpp/build

# Build (Ninja + make available)
RUN cmake .. \
    -DBUILD_GSTREAMER_PLUGIN=ON \
    -GNinja \
    && ninja

# ===================== Runtime =====================
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-ugly \
    openjdk-8-jre \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder \
    /opt/amazon-kinesis-video-streams-producer-sdk-cpp \
    /opt/amazon-kinesis-video-streams-producer-sdk-cpp

WORKDIR /opt/amazon-kinesis-video-streams-producer-sdk-cpp/build

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/
ENV LD_LIBRARY_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/open-source/local/lib
ENV GST_PLUGIN_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build
