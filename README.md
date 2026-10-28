# SyncMesh — Real-Time GenAI Memory Sync Engine

> **Eliminate AI hallucination caused by stale vector memory.**  
> SyncMesh bridges your transactional PostgreSQL database and AI vector memory spaces in real-time using an event-driven Change Data Capture (CDC) pipeline.

---

## Table of Contents

- [What Is SyncMesh?](#what-is-syncmesh)
- [The Problem It Solves](#the-problem-it-solves)
- [Why We Built This](#why-we-built-this)
- [Where It Can Be Used](#where-it-can-be-used)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Getting Started](#getting-started)
- [Running Tests](#running-tests)
- [Performance Targets](#performance-targets)
- [Key Design Decisions](#key-design-decisions)

---

## What Is SyncMesh?

SyncMesh is an **event-driven, asynchronous data synchronisation pipeline** that watches every write (INSERT, UPDATE, DELETE) happening in a PostgreSQL database and automatically reflects those changes into a Qdrant vector database — in near real-time, with sub-second capture latency.

It acts as the **"memory refresh layer"** between your operational database and any AI system that relies on vector search to answer questions about your data.

```
PostgreSQL  ──►  Kafka  ──►  SyncMesh Workers  ──►  Qdrant
 (source)       (bus)         (AI + routing)        (vector memory)
```

---

## The Problem It Solves

### The Data Staleness Window

Modern AI applications — chatbots, copilots, recommendation engines — rely on **Retrieval-Augmented Generation (RAG)**. In RAG, an AI agent searches a vector database to find relevant context before generating an answer.

The critical flaw in most RAG deployments is this:

> **The vector database is updated in batches — once per hour, once per day, or manually.**

This creates a **Data Staleness Window**: a gap of time during which your AI confidently answers questions using outdated, incorrect information — even though the source database already has the correct data.

**Example:**
- A customer's account status is upgraded from `Silver → Platinum` in PostgreSQL at 2:00 PM.
- The nightly vector sync runs at midnight.
- Every AI-powered support query between 2:00 PM and midnight gets the wrong answer: *"Your account is Silver."*
- The AI is not hallucinating from thin air — it is **confidently wrong** because its memory is stale.

### The Tight-Coupling Problem

A naive fix — calling an LLM API directly from a database trigger or application event handler — introduces:

- **Massive latency** on every write operation (LLM calls can take 1–5 seconds)
- **Single points of failure** (if the LLM API is down, your writes fail)
- **No retry or backflow control**
- **No rate-limit handling**

---

## Why We Built This

SyncMesh was built to solve both problems simultaneously with proper engineering principles:

| Problem | SyncMesh Solution |
|---|---|
| Stale vector memory | WAL-based CDC captures mutations in **< 200 ms** |
| Write-path latency | Kafka decouples DB writes from embedding generation **asynchronously** |
| LLM API failures | Dead-Letter Queue (DLQ) ensures **zero event loss** |
| Cloud API rate limits | Redis token-bucket enforces **RPM + TPM budgets** with exponential backoff |
| Vendor lock-in | Adapter pattern allows **seamless Ollama ↔ Gemini switching** via `.env` flag |
| Worker scalability | Kafka consumer groups allow **horizontal scaling** of AI workers |
| Operational reliability | At-least-once delivery + manual offset commit = **no data loss on crashes** |

The goal was not just "make it work" but to make it **production-grade from day one** — observable, fault-tolerant, and horizontally scalable.

---

## Where It Can Be Used

SyncMesh is a general-purpose infrastructure layer. It is relevant anywhere an AI system needs to know about changes in an operational database in real-time.

### 1. 🤖 AI Customer Support Copilots
Keep the AI agent's knowledge of customer accounts, orders, and subscriptions perfectly in sync. When a customer calls, the AI retrieves vector-matched context that reflects the **actual current state** of their account.

### 2. 📄 Enterprise Knowledge Management
Companies store policies, contracts, and documentation in databases. SyncMesh ensures that when a document is updated, the AI assistant's vector memory is updated within seconds — not the next morning.

### 3. 🛒 E-commerce Recommendation Engines
Product catalogues change constantly — prices, availability, descriptions. SyncMesh keeps the recommendation engine's vector index in sync so it never suggests out-of-stock or discontinued products.

### 4. 🏥 Healthcare & Compliance Systems
Patient records, drug formularies, and compliance rules change frequently. Real-time sync ensures that AI-assisted diagnosis tools and compliance checkers always operate on current data.

### 5. 💰 Financial Services & Trading Platforms
Risk profiles, portfolio data, and market regulations update continuously. SyncMesh ensures AI risk advisors and compliance bots reflect the latest state.

### 6. 🏗️ Internal Developer Tools (AIOps / DevOps Copilots)
Incident runbooks, infrastructure states, and service catalogues evolve. SyncMesh keeps the AIOps assistant's memory current so it gives accurate remediation advice.

### 7. 📊 Real-Time Analytics Assistants
Business dashboards and report metadata change as teams update KPIs. SyncMesh keeps the natural-language query assistant in sync with the latest metric definitions.

---

## How It Works

SyncMesh operates as a **four-stage pipeline**:

```
┌─────────────────────────────────────────────────────────────────┐
│                         SYNCMESH PIPELINE                       │
│                                                                 │
│  ┌──────────┐    ┌─────────┐    ┌───────────────┐    ┌───────┐ │
│  │PostgreSQL│    │  Kafka  │    │  Sync Workers  │    │Qdrant │ │
│  │  (WAL)   │───►│ Topics  │───►│ (LLM Routing) │───►│Vector │ │
│  └──────────┘    └─────────┘    └───────────────┘    │  DB   │ │
│                       │               │               └───────┘ │
│                       │          ┌────┴────┐                    │
│                       │          │  Redis  │                    │
│                       │          │ (Rate   │                    │
│                       │          │Limiter) │                    │
│                  ┌────▼────┐     └─────────┘                    │
│                  │   DLQ   │                                     │
│                  │(Retries)│                                     │
│                  └─────────┘                                     │
└─────────────────────────────────────────────────────────────────┘
```

### Stage 1 — CDC Capture (`src/cdc/wal_listener.py`)
- Connects to PostgreSQL via **logical replication** using the `pgoutput` plugin
- Tails the **Write-Ahead Log (WAL)** in real-time — no polling, no triggers
- Parses every mutation into a structured JSON event with `before`, `after`, `operation`, and `timestamp`
- Publishes to a **deterministic Kafka topic** based on the source table name

### Stage 2 — Event Transport (Kafka)
- Events land in Kafka topics partitioned by table name
- Kafka guarantees **at-least-once delivery** and acts as a durable event log
- Workers can be scaled horizontally — all workers in the same consumer group share the load

### Stage 3 — AI Enrichment (`src/consumers/sync_worker.py`)
- Workers consume events concurrently from Kafka
- **INSERT / UPDATE** → builds a deterministic text context → calls LLMClient → upserts embedding into Qdrant
- **DELETE** → bypasses LLM entirely → directly removes vector from Qdrant by primary key
- Failed events are forwarded to the **Dead-Letter Queue** for retry/replay

### Stage 4 — Rate Limiting & LLM Routing (`src/services/llm_client.py` + `src/services/rate_limiter.py`)
- The `LLMClient` checks the `USE_LOCAL_LLM` flag and routes to either **Ollama** (local) or **Gemini** (cloud)
- For cloud calls, `RateLimiter` checks Redis **before** making the API call to enforce RPM/TPM budgets
- Exhausted quotas trigger **exponential backoff** retry — not dropped events

---

## Architecture

```
config/
  settings.py        ← Single source of truth for all env vars
  kafka_config.py    ← Producer/consumer config factories

src/
  cdc/
    wal_listener.py  ← PostgreSQL WAL → Kafka publisher
  consumers/
    base_consumer.py ← Abstract: poll loop, DLQ, offset management
    sync_worker.py   ← Concrete: INSERT/UPDATE/DELETE routing logic
  services/
    llm_client.py    ← Adapter: Ollama (local) | Gemini (cloud)
    rate_limiter.py  ← Redis token-bucket: RPM + TPM enforcement
    vector_store.py  ← Qdrant: upsert / delete / search
  utils/
    logger.py        ← Structured JSON + coloured console logging

tests/
  test_syncmesh.py   ← 49 unit tests, no live infra required
```

---

## Tech Stack

| Component | Technology | Why |
|---|---|---|
| **Source DB** | PostgreSQL 15+ | Industry-standard; native WAL logical replication via `pgoutput` |
| **Event Bus** | Apache Kafka | High-throughput, persistent log; horizontal worker scaling via consumer groups |
| **Rate Limiting** | Redis | Atomic counter operations; sub-millisecond latency for quota checks |
| **AI Processing** | Python 3.11+ | Native async ecosystem; first-class LLM SDK support |
| **Local LLM** | Ollama (`nomic-embed-text`) | Free, offline, no rate limits — perfect for development |
| **Cloud LLM** | Google Gemini (`text-embedding-004`) | Production-grade 768-dim embeddings at scale |
| **Vector Memory** | Qdrant | Rust-based; optimised for high-throughput upserts and cosine similarity search |
| **Containerisation** | Docker + Docker Compose | Reproducible local stack; Kubernetes-ready image |

---

## Project Structure

```
syncmesh/
├── config/
│   ├── kafka_config.py       # Kafka producer/consumer config factories
│   └── settings.py           # Typed env-var loader (singleton)
├── src/
│   ├── __init__.py
│   ├── cdc/
│   │   └── wal_listener.py   # PostgreSQL WAL → Kafka
│   ├── consumers/
│   │   ├── base_consumer.py  # Abstract base: polling, DLQ, offsets
│   │   └── sync_worker.py    # Concrete AI worker
│   ├── services/
│   │   ├── llm_client.py     # Ollama / Gemini adapter
│   │   ├── rate_limiter.py   # Redis token-bucket
│   │   └── vector_store.py   # Qdrant adapter
│   └── utils/
│       └── logger.py         # Structured logger
├── tests/
│   └── test_syncmesh.py      # 49 unit tests (no live infra)
├── .env                      # Environment configuration
├── Dockerfile                # Multi-stage production image
├── docker-compose.yml        # Full local dev stack
└── requirements.txt          # Python dependencies
```

---

## Configuration

All configuration is driven by the `.env` file. Copy and edit before running:

```ini
# ── LLM Provider ─────────────────────────────────────
USE_LOCAL_LLM=true                        # true = Ollama, false = Gemini
OLLAMA_HOST=http://localhost:11434
OLLAMA_EMBEDDING_MODEL=nomic-embed-text

GEMINI_API_KEY=your_production_key_here
GEMINI_EMBEDDING_MODEL=text-embedding-004
GEMINI_RPM_LIMIT=60
GEMINI_TPM_LIMIT=1000000

# ── Kafka ─────────────────────────────────────────────
KAFKA_BROKER=localhost:9092
KAFKA_CDC_TOPIC_PREFIX=syncmesh.cdc
KAFKA_DLQ_TOPIC=syncmesh.dlq
KAFKA_CONSUMER_GROUP=syncmesh-workers

# ── Redis ─────────────────────────────────────────────
REDIS_URL=redis://localhost:6379/0
REDIS_MAX_RETRY_ATTEMPTS=5
REDIS_BASE_BACKOFF_SECONDS=1.0

# ── Qdrant ────────────────────────────────────────────
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION=syncmesh_vectors
QDRANT_VECTOR_SIZE=768

# ── PostgreSQL CDC ────────────────────────────────────
POSTGRES_CDC_URL=postgres://user:pass@localhost:5432/maindb
POSTGRES_REPLICATION_SLOT=syncmesh_slot
POSTGRES_PUBLICATION=syncmesh_pub
POSTGRES_WATCHED_TABLES=            # comma-separated, empty = all tables

# ── Logging ───────────────────────────────────────────
LOG_LEVEL=INFO
LOG_JSON=false                       # true for production log aggregation
```

---

## Getting Started

### Prerequisites
- Docker & Docker Compose installed
- Python 3.11+ (for running tests locally)

### 1. One-command local stack

```bash
# Start all infrastructure + SyncMesh services
docker compose up -d

# View logs from all services
docker compose logs -f

# Scale sync workers (e.g., 5 replicas)
docker compose up -d --scale sync-worker=5
```

### 2. PostgreSQL setup (run once)

Connect to the Postgres container and create the publication:

```sql
-- Enable logical replication (already done via docker-compose flags)
-- Create a publication for all tables
CREATE PUBLICATION syncmesh_pub FOR ALL TABLES;

-- Optional: create a test table
CREATE TABLE customer_profiles (
    id             SERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    account_status TEXT DEFAULT 'Silver',
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);
```

### 3. Test a live sync

```sql
-- This INSERT will flow through the entire pipeline in < 2.5 seconds
INSERT INTO customer_profiles (name, account_status) VALUES ('Alice', 'Gold');

-- UPDATE triggers a vector re-embedding
UPDATE customer_profiles SET account_status = 'Platinum' WHERE name = 'Alice';

-- DELETE removes the vector from Qdrant directly (no LLM call)
DELETE FROM customer_profiles WHERE name = 'Alice';
```

### 4. Switch to Gemini for production

```ini
# In .env — change one line:
USE_LOCAL_LLM=false
GEMINI_API_KEY=your_real_api_key_here
```

Restart the sync-worker service. No code changes required.

---

## Running Tests

The full test suite runs without any live infrastructure — all external I/O is mocked.

```bash
# Install the only test dependency needed locally
pip install python-dotenv

# Run all 49 tests
python -m pytest tests/test_syncmesh.py -v
```

**Expected output:**
```
collected 49 items

tests/test_syncmesh.py::TestSettings::test_kafka_num_partitions_is_int   PASSED
tests/test_syncmesh.py::TestSettings::test_llm_settings_defaults         PASSED
... (49 total)

======================== 49 passed in 0.10s ========================
```

---

## Performance Targets

| Metric | Target | Implementation |
|---|---|---|
| **Ingestion Latency** | < 200 ms | PostgreSQL WAL → Kafka (direct logical replication, no polling) |
| **Processing Latency** | < 2.5 s | Kafka → Qdrant (excludes active LLM network wait) |
| **Availability** | No data loss on worker crash | Kafka offset committed only after successful Qdrant write |
| **Scalability** | Linear horizontal scaling | Kafka consumer groups distribute partitions across workers |

---

## Key Design Decisions

### Why WAL instead of triggers or application-level events?
Database triggers add latency to every write and couple CDC logic to the application schema. WAL-based logical replication is **non-intrusive** — it reads the change log after the fact with zero impact on write performance.

### Why Kafka instead of direct worker calls?
Kafka acts as a **shock absorber**. If LLM APIs are slow or temporarily unavailable, events queue up in Kafka rather than blocking database writes. Workers can be restarted, scaled, or replaced without losing a single event.

### Why Redis for rate limiting instead of in-memory counters?
Multiple sync worker replicas run in parallel. In-memory counters would be **per-process** — each worker would think it has the full quota. Redis provides **distributed atomic counters** so the total request rate across all workers stays within limits.

### Why the adapter pattern for LLM providers?
AI tooling evolves rapidly. Tying business logic to a specific LLM SDK means significant refactoring when switching providers. The adapter pattern means switching from Ollama to Gemini (or any future provider) requires changing **one environment variable**, not one line of code.

### Why manual Kafka offset commits?
`enable.auto.commit=true` would acknowledge an event as processed before the Qdrant write completes. If the worker crashes mid-flight, that event is lost forever. Manual commit after a confirmed Qdrant write guarantees **exactly-once vector updates** from the consumer's perspective.

---

## License

MIT License — free to use, modify, and distribute.

---

*Built with ❤️ to make AI systems that always tell the truth about your data.*
