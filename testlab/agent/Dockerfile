# Образ агента для лаборатории. Промышленный появится в задаче 13.
FROM rust:1.97-alpine AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bin autosre

FROM alpine:3.22
RUN apk add --no-cache git \
 && adduser -D -u 10001 sre \
 && mkdir -p /opt/data/autosre \
 && chown sre /opt/data/autosre
COPY --from=build /src/target/release/autosre /usr/local/bin/autosre
USER sre
EXPOSE 8096
ENTRYPOINT ["/usr/local/bin/autosre"]
