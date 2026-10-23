# The isledb service binary: one image for every role.
#
#   docker run ghcr.io/ankur-anand/isledb:0.19.0 reader --bucket-url … --prefix …
#
# The binary is built for the target platform on the build machine, so a
# multi-platform build needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.25 AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o /out/isledb ./cmd/isledb

# Static: CA certificates for object storage over TLS, and a non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/isledb /usr/local/bin/isledb
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/isledb"]
