# Builder stage uses Docker Official Image
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1-alpine3.23 AS builder

# These are automatically set by Docker Buildx
ARG TARGETOS
ARG TARGETARCH

WORKDIR /go/src/app
COPY --exclude=examples . .
RUN go mod download
# CGO_ENABLED=0 is essential for creating a statically linked binary
# that does not require system libraries (glibc) at runtime
# TARGETOS and TARGETARCH enable cross-compilation for multi-platform builds
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /osm2pgsql-go

# Stage 2: Final (Distroless and Rootless)
# Use a distroless static image, which uses an unprivileged user by default (nonroot:nonroot, UID 65532)
FROM gcr.io/distroless/static-debian12:nonroot

ARG BUILD_DATE
ARG VCS_REF
LABEL org.opencontainers.image.source="https://github.com/wegman-software/osm2pgsql-go"
LABEL org.opencontainers.image.revision=${VCS_REF}
LABEL org.opencontainers.image.created=${BUILD_DATE}
LABEL org.opencontainers.image.base.name="gcr.io/distroless/static-debian12:nonroot"
LABEL org.opencontainers.image.title="osm2pgsql-go"
LABEL org.opencontainers.image.description="Go-based OpenStreetMap to PostgreSQL importer"
LABEL org.opencontainers.image.vendor="Wegman Software"

# Copy the built binary from the 'builder' stage
COPY --from=builder /osm2pgsql-go /osm2pgsql-go
COPY ./examples /styles

# Explicitly set the non-root user for security scanners and compliance
USER nonroot:nonroot

ENTRYPOINT ["/osm2pgsql-go"]
