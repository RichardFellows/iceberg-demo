# Source-Built MinIO Docker Image

This directory contains a custom MinIO Dockerfile that builds MinIO entirely from source code using Red Hat Universal Base Image 8 (UBI8), eliminating all external binary dependencies and making it suitable for the most stringent enterprise security environments.

## Features

- **Source-Built Security**: MinIO server and client compiled from official GitHub source code
- **UBI8 Enterprise Base**: Built on Red Hat Universal Base Image 8.10 for maximum security and compliance
- **Zero External Dependencies**: No binary downloads, no external registry dependencies beyond Red Hat's official UBI
- **Multi-Stage Build**: Efficient builder pattern with separate compilation and runtime stages
- **Production-Ready**: Includes health checks, proper user permissions, and enterprise security practices
- **Compatible**: Drop-in replacement for `quay.io/minio/minio:latest` with enhanced security

## Building the Source-Built Image

```bash
# Build the source-built MinIO image (requires internet access for Go modules and source code)
cd minio/
docker build -t source-built-minio:latest .

# Verify the image and source build
docker images | grep source-built-minio
docker run --rm source-built-minio:latest /usr/local/bin/minio --version
```

**Build Requirements:**
- Internet access for downloading Go modules and MinIO source code
- Sufficient build resources (Go compilation can be CPU/memory intensive)
- Build time: ~5-10 minutes depending on system performance

**Build Process:**
1. **Builder Stage**: Uses UBI8 full image with Go 1.24.6, Git, GCC, and Make
2. **Source Download**: Clones latest MinIO server and client from official GitHub repositories  
3. **Compilation**: Compiles both MinIO server and MC client from source using Red Hat Go toolchain
4. **Runtime Stage**: Copies compiled binaries to clean UBI8-minimal runtime environment

## Usage

### Replace in docker-compose.yml

Change from:
```yaml
minio:
  image: quay.io/minio/minio:latest
```

To:
```yaml
minio:
  build: ./minio
  # OR if pre-built:
  # image: source-built-minio:latest
```

### Environment Variables

The source-built image supports all standard MinIO environment variables:

- `MINIO_ROOT_USER` (default: minioadmin)
- `MINIO_ROOT_PASSWORD` (default: minioadmin123) 
- `MINIO_REGION_NAME` (default: us-east-1)
- `MINIO_DOMAIN`
- `MINIO_SERVER_URL`
- `MINIO_BROWSER_REDIRECT_URL`

## Security Considerations

1. **Source Code Verification**: All MinIO code is compiled from official GitHub repositories with commit verification
2. **UBI8 Base**: Uses Red Hat's hardened Universal Base Image for maximum security compliance
3. **Default Credentials**: The image uses default credentials `minioadmin/minioadmin123` for demo purposes
4. **Production Use**: Always override credentials in production:
   ```yaml
   environment:
     - MINIO_ROOT_USER=your_username
     - MINIO_ROOT_PASSWORD=your_secure_password
   ```
5. **Build Transparency**: Complete build process is transparent - no pre-built binaries from external sources

## Architecture Compatibility

- **Platform**: linux/amd64
- **Base**: Red Hat Universal Base Image 8.10 (RHEL-based)
- **Runtime**: UBI8-minimal for reduced attack surface
- **Compiler**: Red Hat Go 1.24.6 toolchain
- **User**: Non-root user `minio` (uid:1000, gid:1000)
- **Ports**: 9000 (API), 9001 (Console)
- **Size**: ~382MB (larger than binary-based images due to UBI8 base and runtime dependencies)

## Verification

Test the source-built image:

```bash
# Start the source-built MinIO container
docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=testuser \
  -e MINIO_ROOT_PASSWORD=testpass123 \
  source-built-minio:latest

# Verify source build and version
docker exec <container_id> /usr/local/bin/minio --version

# Check health using built-in MinIO client
docker exec <container_id> /usr/local/bin/mc ready local

# Access console
open http://localhost:9001
```

### Source Build Verification

You can verify that MinIO was built from source by checking the version output:

```bash
# Should show DEVELOPMENT version indicating source build
$ docker exec <container_id> /usr/local/bin/minio --version
minio version DEVELOPMENT.2025-08-29T02-39-48Z (commit-id=...)
Runtime: go1.24.6 (Red Hat 1.24.6-1.module+el8.10.0+23407+428597c7) linux/amd64

# Check the base OS
$ docker exec <container_id> cat /etc/os-release
NAME="Red Hat Enterprise Linux"
VERSION="8.10 (Ootpa)"
```

## Integration with Apache Iceberg Demo

This source-built MinIO image provides enterprise-grade security for the Apache Iceberg + Nessie demo:

- **Full S3 Compatibility**: Works seamlessly with Nessie REST catalog and PyIceberg
- **Zero External Dependencies**: No reliance on external binary repositories
- **Enterprise Security**: UBI8 base meets strict enterprise security requirements  
- **Audit Trail**: Complete source code to binary compilation transparency

## Enterprise Benefits

1. **Security Compliance**: Meets requirements for environments that prohibit external binaries
2. **Supply Chain Security**: Full transparency from source code to running container
3. **Airgapped Deployments**: Can be built once and deployed in disconnected environments
4. **Compliance Auditing**: Complete build process documentation and source code verification
5. **Red Hat Support**: Built on supported Red Hat Universal Base Image