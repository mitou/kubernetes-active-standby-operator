# Build a static binary, then ship it on distroless/nonroot for a minimal,
# non-root, read-only-friendly runtime image.
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /operator .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /operator /operator
USER nonroot:nonroot
ENTRYPOINT ["/operator"]
