# Образ агента. Секретов внутри нет и быть не может: они приходят из окружения
# ([ADR-0025](docs/adr/0025-config-file-and-secrets.md)).

FROM rust:1.97-alpine AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bin autosre

FROM alpine:3.22
RUN apk add --no-cache ca-certificates git \
 && adduser -D -u 10001 sre \
 && mkdir -p /opt/data/autosre \
 && chown sre /opt/data/autosre
COPY --from=build /src/target/release/autosre /usr/local/bin/autosre
USER sre
EXPOSE 8096
# Проверка здоровья вшита в образ: снаружи её пришлось бы описывать в каждом
# compose и в роли — и рано или поздно они разошлись бы.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD wget -qO- --no-check-certificate https://127.0.0.1:8096/api/health || exit 1
ENTRYPOINT ["/usr/local/bin/autosre"]
CMD ["/etc/autosre/autosre.toml"]
