# Custom MinIO Dockerfile - Build from source using UBI8
# Based on Red Hat Universal Base Image 8
FROM registry.access.redhat.com/ubi8/ubi:latest AS builder

# Install Go and build dependencies
RUN dnf install -y \
    golang \
    git \
    gcc \
    make \
    ca-certificates \
    && dnf clean all

# Set Go environment variables
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
ENV GO111MODULE=on

# Create workspace
WORKDIR /go/src/github.com/minio

# Clone MinIO server source code
RUN git clone --depth=1 https://github.com/minio/minio.git && \
    cd minio && \
    git submodule update --init --recursive

# Build MinIO server from source
WORKDIR /go/src/github.com/minio/minio
RUN make build && \
    cp minio /usr/local/bin/minio && \
    chmod +x /usr/local/bin/minio

# Clone and build MinIO client (mc) from source
WORKDIR /go/src/github.com/minio
RUN git clone --depth=1 https://github.com/minio/mc.git && \
    cd mc && \
    make build && \
    cp mc /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Runtime stage - Clean UBI8 image
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

# Set environment variables
ENV MINIO_ROOT_USER="" \
    MINIO_ROOT_PASSWORD="" \
    MINIO_REGION_NAME="us-east-1" \
    MINIO_DOMAIN="" \
    MINIO_SERVER_URL="" \
    MINIO_BROWSER_REDIRECT_URL=""

# Install runtime dependencies including util-linux for runuser
RUN microdnf update && \
    microdnf install -y ca-certificates shadow-utils util-linux && \
    microdnf clean all

# Create minio user and group
RUN groupadd -g 1000 minio && \
    useradd -u 1000 -g 1000 -s /bin/bash -m minio

# Copy built binaries from builder stage
COPY --from=builder /usr/local/bin/minio /usr/local/bin/minio
COPY --from=builder /usr/local/bin/mc /usr/local/bin/mc

# Create data directory
RUN mkdir -p /data && \
    chown minio:minio /data

# Create entrypoint script that handles permissions
RUN cat > /usr/local/bin/docker-entrypoint.sh << 'EOF'
#!/bin/bash

# Set default credentials if not provided
export MINIO_ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
export MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin123}

# Fix data directory permissions
chown -R minio:minio /data
chmod -R u+rwX /data

# Switch to minio user and start MinIO server
exec runuser -u minio -- /usr/local/bin/minio "$@"
EOF

RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose MinIO ports
EXPOSE 9000 9001

# Health check using built-in curl alternative or mc
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD /usr/local/bin/mc ready local || exit 1

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["server", "/data", "--console-address", ":9001"]