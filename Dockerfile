# ── Build stage ────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies for psycopg2 + confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# ── Runtime stage ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Copy only the runtime libraries from builder — keeps the image lean
COPY --from=builder /usr/local/lib/python3.11/site-packages \
                    /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy project source
COPY config/ ./config/
COPY src/    ./src/

# Run as non-root for security
RUN useradd --create-home --shell /bin/bash syncmesh
USER syncmesh

# ── Labels ────────────────────────────────────────────────────────────────
LABEL maintainer="SyncMesh Team"
LABEL description="Real-Time GenAI Memory Sync Engine"

# Default command — override in docker-compose per service
CMD ["python", "-m", "src.consumers.sync_worker"]
