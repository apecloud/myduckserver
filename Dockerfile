# Step 1: Build stage
# Use the official Go image for building the Go application
FROM golang:1.23 AS builder

# Set environment variables for cross-compilation
ARG TARGETOS=linux
ARG TARGETARCH
ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH

# Set the working directory inside the container
WORKDIR /app

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the remaining source code
COPY . .

# Build the Go application for the target OS and architecture
RUN go build -o /myduckserver

# Step 2: Final stage
FROM debian:bookworm-slim

ARG TARGETOS=linux
ARG TARGETARCH

# Install curl, libstdc++6, and other necessary tools
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Dynamic DuckDB CLI download based on architecture
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        ARCH="aarch64"; \
    else \
        ARCH="amd64"; \
    fi && \
    curl -LJO https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-$ARCH.zip \
    && unzip duckdb_cli-linux-$ARCH.zip \
    && chmod +x duckdb \
    && mv duckdb /usr/local/bin \
    && rm duckdb_cli-linux-$ARCH.zip \
    && duckdb -c 'SELECT extension_name, loaded, install_path FROM duckdb_extensions() where installed'

RUN duckdb -version

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir "sqlglot[rs]"  --break-system-packages

# Set the working directory inside the container
WORKDIR /app

# Copy the compiled Go binary from the builder stage
COPY --from=builder /myduckserver /usr/local/bin/myduckserver

ENV LC_CTYPE="en_US.UTF-8"
ENV LANG="en_US.UTF-8"

# Expose the port your server will run on (if applicable)
EXPOSE 3306

# Set the default command to run the Go server
CMD ["myduckserver"]