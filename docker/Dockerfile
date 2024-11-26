# Use the official Go image for building the Go application
# Step 1: Build stage
FROM --platform=${BUILDPLATFORM} golang:1.23.2 AS builder

# Set environment variables for cross-compilation
ARG TARGETOS
ARG TARGETARCH
ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH

# Set the working directory inside the container
WORKDIR /app

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy the remaining source code
COPY . .

RUN apt-get update && \
    apt-get install -y debian-archive-keyring && \
    apt-get update && \
    apt-get install -y \
    gcc-aarch64-linux-gnu \
    g++-aarch64-linux-gnu \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Build the Go application for the target OS and architecture
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

ARG TARGETOS
ARG TARGETARCH

RUN apt-get update && apt-get install -y debian-archive-keyring \
    && apt-get update && apt-get install -y \
    vim \
    procps \
    curl \
    unzip \
    libstdc++6 \
    python3 python3-pip \
    libpq-dev postgresql-client \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir "sqlglot[rs]" --break-system-packages

# Install mysql-shell
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        ARCH="arm"; \
    else \
        ARCH="x86"; \
    fi && \
    curl -LJO https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell-9.1.0-linux-glibc2.28-${ARCH}-64bit.tar.gz \
    && tar -zxvf mysql-shell-9.1.0-linux-glibc2.28-${ARCH}-64bit.tar.gz \
    && rm mysql-shell-9.1.0-linux-glibc2.28-${ARCH}-64bit.tar.gz \
    && mv mysql-shell-9.1.0-linux-glibc2.28-${ARCH}-64bit /usr/local/mysqlsh \
    && ln -s /usr/local/mysqlsh/bin/mysqlsh /usr/local/bin/mysqlsh \
    && mysqlsh --version

# Dynamic DuckDB CLI download based on architecture
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        ARCH="aarch64"; \
    else \
        ARCH="amd64"; \
    fi && \
    curl -LJO https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-$ARCH.zip \
    && unzip duckdb_cli-linux-$ARCH.zip \
    && chmod +x duckdb \
    && mv duckdb /usr/local/bin \
    && rm duckdb_cli-linux-$ARCH.zip \
    && duckdb -c 'SELECT extension_name, loaded, install_path FROM duckdb_extensions() where installed'

RUN duckdb -version

RUN useradd --create-home --user-group --shell /bin/bash admin \
    && echo 'admin:admin' | chpasswd \
    && echo 'admin ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Set the working directory inside the container
USER admin
WORKDIR /home/admin

# Copy the compiled Go binary from the builder stage
COPY --from=builder /myduckserver /usr/local/bin/myduckserver
COPY --chown=admin:admin --chmod=755 docker/*.sh .
COPY --chown=admin:admin --chmod=755 devtools/replica-setup-mysql ./replica-setup-mysql
COPY --chown=admin:admin --chmod=755 devtools/replica-setup-postgres ./replica-setup-postgres

# ENV LC_CTYPE="en_US.UTF-8"
# ENV LANG="en_US.UTF-8"

# Install the required DuckDB extensions
RUN myduckserver --init

# Expose the ports your server will run on (if applicable)
EXPOSE 3306
EXPOSE 5432

# Set the default command to run the Go server
ENTRYPOINT /home/admin/entrypoint.sh