# Multi-stage Rust build：
# 1. builder：cargo build --release（缓存依赖 + 全 binaries）
# 2. runtime：debian-slim，CA certs + libssl，跑 release binaries
#
# 体积目标：runtime image ~80MB（vs Bun 镜像 ~250MB）

FROM rust:1-slim-bookworm AS builder
WORKDIR /app

# 系统依赖（rustls 用 ring，不需要 libssl-dev；为 reqwest features 已切 rustls-tls）
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config build-essential libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 先 copy Cargo.toml 缓存依赖（如果不变就走 cache）
COPY Cargo.toml ./
COPY Cargo.lock ./
# stub source 让 cargo 解析依赖
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/bin/backfill.rs && \
    echo "fn main() {}" > src/bin/radar.rs && \
    echo "fn main() {}" > src/bin/verify_factories.rs && \
    echo "fn main() {}" > src/bin/enrich_bsc_mapping.rs && \
    echo "" > src/lib.rs && \
    cargo build --release || true && \
    rm -rf src

# copy 全部源码 + 实际 build
COPY src ./src
RUN touch src/lib.rs src/bin/*.rs && \
    cargo build --release --bins

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# 拷贝 binaries
COPY --from=builder /app/target/release/backfill /usr/local/bin/backfill
COPY --from=builder /app/target/release/radar /usr/local/bin/radar
COPY --from=builder /app/target/release/verify-factories /usr/local/bin/verify-factories
COPY --from=builder /app/target/release/enrich-bsc-mapping /usr/local/bin/enrich-bsc-mapping

# 静态资源
COPY seed ./seed
COPY db ./db

ENV RUST_LOG=info

# 默认启动 radar（stream + detector）
CMD ["/usr/local/bin/radar"]
