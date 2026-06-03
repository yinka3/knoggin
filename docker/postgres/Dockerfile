FROM apache/age:release_PG16_1.6.0

# Switch to root to install pgvector
USER root

ARG PG_MAJOR=16
ARG PGVECTOR_VERSION=v0.8.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-server-dev-${PG_MAJOR} \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN test "$(pg_config --version | awk '{print $2}' | cut -d. -f1)" = "${PG_MAJOR}"

# Clone and build pgvector
RUN git clone --branch "${PGVECTOR_VERSION}" https://github.com/pgvector/pgvector.git /tmp/pgvector \
    && cd /tmp/pgvector \
    && make \
    && make install \
    && rm -rf /tmp/pgvector
