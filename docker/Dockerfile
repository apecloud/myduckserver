# Step 1: Build stage
FROM --platform=${BUILDPLATFORM} golang:latest AS builder

ARG TARGETOS
ARG TARGETARCH
ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y debian-archive-keyring curl unzip \
    gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Download MySQL Shell and DuckDB CLI in builder stage
RUN mkdir -p /downloads && cd /downloads && \
    if [ "$TARGETARCH" = "arm64" ]; then \
        curl -LJO https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell-9.1.0-linux-glibc2.28-arm-64bit.tar.gz && \
        tar -zxf mysql-shell-9.1.0-linux-glibc2.28-arm-64bit.tar.gz && \
        mv mysql-shell-9.1.0-linux-glibc2.28-arm-64bit mysqlsh && \
        touch mysql-shell.deb && \
        ARCH="aarch64"; \
    else \
        curl -LJO https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell_9.1.0-1debian12_amd64.deb && \
        mv mysql-shell_9.1.0-1debian12_amd64.deb mysql-shell.deb && \
        mkdir mysqlsh && \
        ARCH="amd64"; \
    fi && \
    curl -LJO https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-$ARCH.zip && \
    unzip duckdb_cli-linux-$ARCH.zip && \
    chmod +x duckdb

# Build Go application
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go env && \
    if [ "$TARGETARCH" = "arm64" ]; then \
      CC="aarch64-linux-gnu-gcc" CXX="aarch64-linux-gnu-g++" CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /myduckserver; \
    else \
      CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /myduckserver; \
    fi

# Step 2: Final stage
FROM debian:bookworm-slim

ARG TARGETARCH

RUN apt-get update && apt-get install -y debian-archive-keyring \
    && apt-get update && apt-get install -y \
    libssh-4 \
    python3 python3-pip \
    postgresql-client \
    --no-install-recommends \
    && pip install --no-cache-dir "sqlglot[rs]" --break-system-packages \
    && rm -rf /var/lib/apt/lists/*

# Install MySQL Shell from builder stage
COPY --from=builder /downloads/duckdb /usr/local/bin/
COPY --from=builder /downloads/mysqlsh /usr/local/mysqlsh.arm64
COPY --from=builder /downloads/mysql-shell.deb /tmp/

RUN if [ "$TARGETARCH" = "arm64" ]; then \
        mv /usr/local/mysqlsh.arm64 /usr/local/mysqlsh && \
        ln -s /usr/local/mysqlsh/bin/mysqlsh /usr/local/bin/mysqlsh && \
        rm -f /tmp/mysql-shell.deb; \
    else \
        apt-get update && \
        apt-get install -y /tmp/mysql-shell.deb && \
        rm /tmp/mysql-shell.deb && \
        rm -rf /usr/local/mysqlsh.arm64 && \
        rm -rf /var/lib/apt/lists/*; \
    fi && \
    rm -f /usr/local/mysqlsh.arm64

# Setup user and working directory, and create init directories
RUN useradd --create-home --user-group --shell /bin/bash admin \
    && echo 'admin:admin' | chpasswd \
    && echo 'admin ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
    && mkdir -p /docker-entrypoint-initdb.d/mysql /docker-entrypoint-initdb.d/postgres

USER admin
WORKDIR /home/admin

# Copy application files
COPY --from=builder /myduckserver /usr/local/bin/myduckserver
COPY --chown=admin:admin --chmod=755 docker/*.sh .
COPY --chown=admin:admin --chmod=755 devtools/replica-setup-mysql ./replica-setup-mysql
COPY --chown=admin:admin --chmod=755 devtools/replica-setup-postgres ./replica-setup-postgres

# Initialize DuckDB
RUN myduckserver --init

EXPOSE 3306 5432
ENTRYPOINT /home/admin/entrypoint.sh
