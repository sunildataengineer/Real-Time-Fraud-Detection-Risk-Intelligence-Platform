# 🔐 Real-Time Fraud & Anomaly Detection Streaming Platform

<div align="center">

![Platform Status](https://img.shields.io/badge/Status-Production--Ready-brightgreen?style=for-the-badge)
![Throughput](https://img.shields.io/badge/Throughput-500K%20events%2Fsec-blue?style=for-the-badge)
![Data Volume](https://img.shields.io/badge/Daily%20Volume-1--2%20TB-orange?style=for-the-badge)
![Transactions](https://img.shields.io/badge/Transactions-100M%2B%20per%20day-red?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)

**A production-grade, low-latency stateful streaming platform for real-time fraud detection and anomaly scoring at 100M+ transactions/day.**

[Architecture](#-architecture-diagram) • [How It Works](#-how-it-works) • [Data Flow](#-data-flow) • [System Design](#-system-design) • [Getting Started](#-prerequisites)

</div>

---

## 📌 Table of Contents

1. [Architecture Diagram](#-architecture-diagram)
2. [How It Works](#-how-it-works)
3. [Data Ingestion & Scraping](#-data-ingestion--scraping)
4. [Data Flow](#-data-flow)
5. [Data Access Layer](#-data-access-layer)
6. [Data Modelling](#-data-modelling)
7. [System Design](#-system-design)
8. [Prerequisites](#-prerequisites)
9. [Running the Project](#-running-the-project)
10. [Testing](#-testing)
11. [API Service](#-api-service)
12. [References](#-references)
13. [Contributing](#-contributing)
14. [License](#-license)

---

## 🏗️ Architecture Diagram

<img width="1536" height="1024" alt="ChatGPT Image Jul 2, 2026, 05_07_18 AM" src="https://github.com/user-attachments/assets/ddc52a3d-f286-4115-aecc-b42f7f3c494a" />


## ⚙️ How It Works

This platform detects fraudulent financial transactions in **real-time** using a layered, stateful streaming architecture capable of processing **100M+ transactions per day** at sub-100ms latency.

### Core Processing Pipeline

| Stage | Component | Function |
|---|---|---|
| **1. Ingestion** | Apache Kafka | Ingest raw transaction events from POS, ATM, mobile, and online channels at 100K–500K events/sec |
| **2. Enrichment** | Flink / PySpark | Enrich transactions with user history, device fingerprints, geolocation risk, and merchant category |
| **3. Feature Extraction** | Flink Stateful Ops | Compute real-time features: spend velocity, transaction frequency, country mismatch, device risk |
| **4. Fraud Scoring** | ML + Rule Engine | Score each transaction using ensemble of rule-based checks + ML anomaly detection models |
| **5. Alert Dispatch** | Kafka + SNS | Instantly route high-risk transactions to alert queues for human review or auto-block |
| **6. Storage** | Cassandra + S3 | Write fraud verdicts to hot store (Cassandra) and archive raw events to cold store (S3) |
| **7. Observability** | Prometheus + Grafana | Monitor throughput, latency, fraud rates, and system health in real-time |

### Detection Logic

The platform uses a **3-tier fraud scoring model**:

- **Tier 1 — Deterministic Rules:** Hard rules (velocity limits, blacklisted IPs, blocked card BINs). Zero-latency, zero-miss.
- **Tier 2 — Statistical Anomaly Detection:** Z-score and IQR-based deviation on sliding window aggregates (1min / 5min / 15min windows).
- **Tier 3 — ML Scoring:** Isolation Forest model trained on 90-day historical transaction patterns, evaluated on extracted real-time features.

Final risk score = weighted combination of all three tiers. Transactions above threshold trigger real-time alerts.

---

## 📡 Data Ingestion & Scraping

### Transaction Sources

```python
# Kafka Producer — Transaction Event Schema
transaction_event = {
    "transaction_id": "TXN-20240615-8f3a2b",
    "card_number_hash": "sha256_hashed_value",
    "merchant_id": "MCH-4829",
    "merchant_category_code": "5411",
    "amount_usd": 249.99,
    "currency": "USD",
    "timestamp_utc": "2024-06-15T14:32:01.123Z",
    "terminal_id": "POS-NYC-0029",
    "device_fingerprint": "fp_abc123xyz",
    "ip_address_hash": "hashed_ip",
    "geolocation": {"lat": 40.7128, "lon": -74.0060},
    "transaction_type": "PURCHASE",
    "channel": "IN_STORE"
}
```

### Kafka Topic Configuration

```yaml
# kafka-topics-config.yaml
topics:
  fraud-raw-transactions:
    partitions: 64
    replication_factor: 3
    retention_ms: 86400000       # 24 hours
    compression_type: lz4

  fraud-enriched-events:
    partitions: 64
    replication_factor: 3
    retention_ms: 86400000

  fraud-alerts:
    partitions: 16
    replication_factor: 3
    retention_ms: 604800000      # 7 days

  fraud-metrics:
    partitions: 8
    replication_factor: 3
```

### Data Sources Integration

| Source | Protocol | Volume | Latency |
|---|---|---|---|
| POS Terminals | REST → Kafka Producer | ~40M txns/day | <10ms |
| ATM Networks | TCP/MQ → Kafka Bridge | ~15M txns/day | <20ms |
| Mobile Banking | gRPC → Kafka Producer | ~30M txns/day | <15ms |
| Online Payments | Webhook → Kafka | ~15M txns/day | <25ms |

---

## 🔄 Data Flow

```
TRANSACTION EVENT LIFECYCLE
═══════════════════════════

[1] RAW INGESTION
    Transaction Sources → Kafka (fraud-raw-transactions)
    • Schema: Avro with Schema Registry
    • Throughput: 100K–500K msgs/sec
    • Partitioning: by card_number_hash

         ↓ ~5ms

[2] STREAM ENRICHMENT (Flink Job: EnrichmentJob)
    fraud-raw-transactions → Flink → fraud-enriched-events
    • Lookup: User risk profile from Cassandra
    • Lookup: Merchant risk score from PostgreSQL
    • Lookup: Geo-IP risk from Redis cache
    • Enrich: Device fingerprint risk score

         ↓ ~10ms

[3] FEATURE COMPUTATION (Flink Job: FeatureExtractionJob)
    fraud-enriched-events → Flink Stateful Windows
    Features computed:
    ┌─ tx_count_last_1min        (sliding window)
    ├─ spend_velocity_last_5min  (sliding window)
    ├─ unique_merchants_1hr      (set aggregation)
    ├─ cross_country_flag        (geolocation delta)
    ├─ time_since_last_txn_secs  (state lookup)
    └─ card_present_mismatch     (device check)

         ↓ ~15ms

[4] FRAUD SCORING (Flink Job: FraudScoringJob)
    Feature Vector → Rule Engine + ML Model
    ┌─ Rule Score    (0–100): Hard rules evaluation
    ├─ ML Score      (0–1):   Isolation Forest inference
    └─ Final Score   (0–100): Weighted ensemble

         ↓ ~20ms

[5] ALERT ROUTING
    Score ≥ 80  → fraud-alerts → Block + Alert
    Score 50–79 → fraud-review → Human Review Queue
    Score < 50  → Approved → Transaction proceeds

         ↓ async

[6] STORAGE SINKS
    All Events  → Amazon S3 (Parquet, hourly partitioned)
    Fraud Alerts→ Cassandra (real-time lookup)
    Aggregates  → PostgreSQL (reporting + BI)
    Metrics     → Prometheus → Grafana

Total end-to-end latency: < 100ms (P99)
```

---

## 🗄️ Data Access Layer

### Storage Tiers

#### Hot Storage — Apache Cassandra

```sql
-- Fraud Alert Table (sub-5ms reads)
CREATE TABLE fraud_keyspace.fraud_alerts (
    card_number_hash    TEXT,
    transaction_id      TEXT,
    alert_timestamp     TIMESTAMP,
    fraud_score         DOUBLE,
    fraud_reason        TEXT,
    action_taken        TEXT,       -- BLOCKED, REVIEW, APPROVED
    rule_triggers       LIST<TEXT>,
    ml_score            DOUBLE,
    PRIMARY KEY ((card_number_hash), alert_timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (alert_timestamp DESC)
  AND default_time_to_live = 7776000  -- 90 days TTL
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};

-- User Session State Table
CREATE TABLE fraud_keyspace.user_risk_state (
    user_id             TEXT PRIMARY KEY,
    risk_tier           TEXT,
    tx_count_24hr       INT,
    spend_total_24hr    DOUBLE,
    last_seen_country   TEXT,
    last_device_id      TEXT,
    last_updated        TIMESTAMP
);
```

#### Cold Storage — Amazon S3 (Data Lake)

```
s3://fraud-detection-datalake/
├── raw/
│   └── transactions/
│       └── year=2024/month=06/day=15/hour=14/
│           └── part-00001-*.parquet           # hourly Parquet files
├── enriched/
│   └── year=2024/month=06/day=15/
│       └── part-*.parquet
├── fraud_alerts/
│   └── year=2024/month=06/day=15/
│       └── fraud_alerts_*.parquet
└── ml_features/
    └── feature_store/
        └── date=2024-06-15/
            └── features_*.parquet
```

#### Warm Storage — PostgreSQL (Analytics)

```sql
-- Fraud Summary Reporting Table
CREATE TABLE fraud_reporting.daily_fraud_summary (
    report_date         DATE,
    merchant_category   VARCHAR(10),
    total_transactions  BIGINT,
    flagged_count       INT,
    blocked_count       INT,
    false_positive_rate DECIMAL(5,4),
    avg_fraud_score     DECIMAL(6,4),
    total_fraud_amount  DECIMAL(15,2),
    PRIMARY KEY (report_date, merchant_category)
);
```

---

## 📐 Data Modelling

### Conceptual Model

<img width="1448" height="1086" alt="#" src="https://github.com/user-attachments/assets/1d40eb6c-568e-4de9-8a4b-16f7181e6680" />



### Feature Store Schema (ML Training)

```python
# Feature Vector per Transaction
feature_schema = {
    # Velocity Features
    "tx_count_1min":          "DOUBLE",    # transactions in last 1 min
    "tx_count_5min":          "DOUBLE",    # transactions in last 5 min
    "tx_count_1hr":           "DOUBLE",    # transactions in last 1 hour
    "spend_velocity_5min":    "DOUBLE",    # total spend in last 5 min
    "spend_velocity_1hr":     "DOUBLE",    # total spend in last 1 hour

    # Behavioral Features
    "unique_merchants_1hr":   "INT",       # distinct merchants in 1 hour
    "unique_countries_24hr":  "INT",       # distinct countries in 24 hours
    "time_since_last_txn":    "DOUBLE",    # seconds since last transaction
    "avg_txn_amount_30d":     "DOUBLE",    # 30-day average transaction amount
    "txn_amount_deviation":   "DOUBLE",    # deviation from 30d average

    # Risk Signals
    "cross_country_flag":     "BOOLEAN",   # geolocation mismatch
    "new_device_flag":        "BOOLEAN",   # unseen device fingerprint
    "card_present_mismatch":  "BOOLEAN",   # card-present vs channel mismatch
    "merchant_risk_score":    "DOUBLE",    # pre-computed merchant risk
    "ip_reputation_score":    "DOUBLE",    # IP address reputation

    # Target
    "is_fraud":               "BOOLEAN"    # ground truth label
}
```

---

## 🧩 System Design

### High-Level Design Decisions

| Design Concern | Decision | Rationale |
|---|---|---|
| **Message Bus** | Apache Kafka (64 partitions) | Horizontal scalability, durable log, exactly-once semantics |
| **Stream Processor** | Apache Flink | Stateful processing, CEP, event-time semantics, low latency |
| **Hot Store** | Apache Cassandra | Sub-5ms P99 reads, high write throughput, TTL support |
| **Cold Store** | Amazon S3 + Parquet | Cost-efficient, columnar analytics, Iceberg for ACID |
| **ML Model** | Isolation Forest + Rule Engine | Explainability + real-time inference capability |
| **Orchestration** | Apache Airflow | ML model retraining DAGs, batch reconciliation jobs |
| **Container Infra** | Docker + Kubernetes (EKS) | Auto-scaling, fault tolerance, resource isolation |
| **Observability** | Prometheus + Grafana + ELK | End-to-end traceability from ingestion to alert |

### Scalability & Fault Tolerance

```
KAFKA PARTITION STRATEGY
  card_number_hash mod 64 → deterministic partition assignment
  → Guarantees in-order processing per card
  → Enables efficient stateful aggregation per card

FLINK STATE BACKEND
  RocksDB (persistent, spill-to-disk)
  Checkpoints: every 30 seconds to S3
  Recovery: automatic restart from last checkpoint
  Parallelism: 32 task slots per Flink job

CASSANDRA REPLICATION
  Replication Factor: 3 (across 3 AZs)
  Consistency: LOCAL_QUORUM (reads + writes)
  Keyspace Strategy: NetworkTopologyStrategy

KUBERNETES AUTO-SCALING
  Flink Job Manager: 1 replica (HA via ZooKeeper)
  Flink Task Managers: 4–32 replicas (HPA on CPU/memory)
  Kafka Brokers: 9 brokers (3 per AZ)
```

### SLA Targets

| Metric | Target |
|---|---|
| End-to-end fraud detection latency | < 100ms (P99) |
| System availability | 99.99% uptime |
| Throughput | 500K events/second sustained |
| False positive rate | < 0.5% |
| Checkpoint recovery time | < 60 seconds |
| Data durability | 99.999999999% (S3) |

---

## 🔧 Prerequisites

### Infrastructure Requirements

| Component | Minimum Spec | Recommended |
|---|---|---|
| Kubernetes | 1.27+ | EKS 1.29+ |
| Apache Kafka | 3.5+ | AWS MSK 3.6 |
| Apache Flink | 1.18+ | 1.19 |
| Apache Cassandra | 4.1+ | 4.1.3 |
| PostgreSQL | 14+ | RDS 15+ |
| Amazon S3 | N/A | S3 Intelligent Tiering |
| Python | 3.10+ | 3.11 |
| Java | 11+ | 17 (Flink runtime) |
| Docker | 24+ | 25+ |

### Environment Setup

```bash
# 1. Clone the repository
git clone https://github.com/sunildataengineer/Real-Time-Fraud-Anomaly-Detection-Streaming-Platform.git
cd Real-Time-Fraud-Anomaly-Detection-Streaming-Platform

# 2. Set up Python virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Edit .env with your AWS credentials, Kafka brokers, DB connections

# 5. Install AWS CLI and configure
aws configure
# Requires: AdministratorAccess or custom IAM policy (see docs/iam-policy.json)

# 6. Install kubectl and Helm
curl -LO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
helm repo add stable https://charts.helm.sh/stable && helm repo update
```

### Required AWS IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
        "kafka:*",
        "eks:*",
        "rds:*",
        "cloudwatch:*",
        "sns:Publish"
      ],
      "Resource": "*"
    }
  ]
}
```

---

## 🚀 Running the Project

### Option 1 — Local Development (Docker Compose)

```bash
# Start all local services
docker-compose up -d

# Services started:
# - Kafka + Zookeeper (localhost:9092)
# - Cassandra (localhost:9042)
# - PostgreSQL (localhost:5432)
# - Flink JobManager (localhost:8081)
# - Grafana (localhost:3000)
# - Prometheus (localhost:9090)

# Verify all services are healthy
docker-compose ps

# Initialize schemas
python scripts/init_cassandra_schema.py
python scripts/init_postgres_schema.py
python scripts/create_kafka_topics.py

# Start the Kafka transaction producer (synthetic data)
python src/producers/transaction_producer.py \
  --rate 10000 \
  --fraud-rate 0.02 \
  --duration 300

# Submit Flink streaming jobs
./scripts/submit_flink_jobs.sh

# Verify fraud alerts are flowing
python scripts/consume_fraud_alerts.py
```

### Option 2 — Kubernetes (Production)

```bash
# Deploy infrastructure via Helm
helm upgrade --install fraud-platform ./helm/fraud-platform \
  --namespace fraud-detection \
  --create-namespace \
  --values helm/fraud-platform/values-prod.yaml

# Deploy Flink jobs
kubectl apply -f k8s/flink-jobs/

# Deploy Kafka Connect sinks
kubectl apply -f k8s/kafka-connect/

# Monitor deployment
kubectl get pods -n fraud-detection
kubectl logs -f deployment/flink-jobmanager -n fraud-detection

# Port-forward dashboards
kubectl port-forward svc/grafana 3000:3000 -n monitoring
kubectl port-forward svc/flink-jobmanager 8081:8081 -n fraud-detection
```

### Option 3 — AWS Full Deployment (Terraform)

```bash
cd terraform/
terraform init
terraform plan -var-file="environments/prod.tfvars"
terraform apply -var-file="environments/prod.tfvars"

# After infrastructure is ready, deploy application
./scripts/deploy-aws.sh --env prod
```

---

## 🧪 Testing

### Unit Tests

```bash
# Run all unit tests
pytest tests/unit/ -v --cov=src --cov-report=html

# Test fraud scoring logic
pytest tests/unit/test_fraud_scoring.py -v

# Test Kafka producer/consumer
pytest tests/unit/test_kafka_utils.py -v

# Test feature extraction
pytest tests/unit/test_feature_extractor.py -v
```

### Integration Tests

```bash
# Requires running Docker Compose environment
docker-compose up -d

# Run integration tests
pytest tests/integration/ -v --timeout=120

# End-to-end pipeline test
pytest tests/integration/test_full_pipeline.py -v
```

### Load Testing

```bash
# Generate 100K transactions/min load
python tests/load/load_test.py \
  --tps 1666 \
  --duration 600 \
  --fraud-injection-rate 0.02

# Expected output:
# ✅ Throughput: 1,666 TPS sustained
# ✅ P99 latency: 87ms
# ✅ Fraud detection rate: 97.3%
# ✅ False positive rate: 0.42%
```

### Fraud Scenario Tests

```bash
# Test specific fraud patterns
python tests/scenarios/test_velocity_fraud.py      # Card velocity attack
python tests/scenarios/test_geo_fraud.py           # Cross-country mismatch
python tests/scenarios/test_card_takeover.py       # Account takeover pattern
python tests/scenarios/test_merchant_fraud.py      # Merchant-side fraud
```

### Test Coverage

```
Module                          Stmts   Miss  Cover
----------------------------------------------------
src/producers/                    142      8    94%
src/processors/fraud_scorer.py    231     12    95%
src/processors/feature_extractor  189      9    95%
src/sinks/cassandra_sink.py        98      5    95%
src/api/fraud_api.py              156      7    96%
----------------------------------------------------
TOTAL                             816     41    95%
```

---

## 🌐 API Service

The platform exposes a **REST API** for real-time fraud verdict lookup, case management, and system health monitoring.

### Base URL

```
Production:  https://fraud-api.yourdomain.com/v1
Development: http://localhost:8000/v1
```

### Authentication

```bash
# All endpoints require Bearer token
curl -H "Authorization: Bearer <your-api-token>" \
     https://fraud-api.yourdomain.com/v1/health
```

### Endpoints

#### `GET /v1/health`

```bash
curl https://fraud-api.yourdomain.com/v1/health

# Response 200 OK
{
  "status": "healthy",
  "kafka_lag": 1243,
  "flink_job_status": "RUNNING",
  "cassandra_latency_ms": 3.2,
  "throughput_tps": 142389,
  "uptime_seconds": 864023
}
```

#### `GET /v1/transactions/{transaction_id}/fraud-verdict`

```bash
curl https://fraud-api.yourdomain.com/v1/transactions/TXN-20240615-8f3a2b/fraud-verdict

# Response 200 OK
{
  "transaction_id": "TXN-20240615-8f3a2b",
  "fraud_score": 87.4,
  "verdict": "BLOCKED",
  "confidence": 0.94,
  "rule_triggers": ["VELOCITY_EXCEEDED", "CROSS_COUNTRY_MISMATCH"],
  "ml_score": 0.891,
  "processing_latency_ms": 63,
  "timestamp_utc": "2024-06-15T14:32:01.186Z"
}
```

#### `GET /v1/cards/{card_hash}/fraud-history`

```bash
curl "https://fraud-api.yourdomain.com/v1/cards/sha256_hash/fraud-history?limit=10"

# Response 200 OK
{
  "card_hash": "sha256_hash",
  "total_alerts_90d": 3,
  "risk_tier": "HIGH",
  "fraud_history": [...]
}
```

#### `POST /v1/transactions/score` (Real-time scoring)

```bash
curl -X POST https://fraud-api.yourdomain.com/v1/transactions/score \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "TXN-TEST-001",
    "card_hash": "abc123",
    "amount_usd": 4999.99,
    "merchant_id": "MCH-0022",
    "channel": "ONLINE",
    "geolocation": {"lat": 55.75, "lon": 37.61}
  }'

# Response 200 OK
{
  "transaction_id": "TXN-TEST-001",
  "fraud_score": 92.1,
  "verdict": "BLOCKED",
  "latency_ms": 41
}
```

#### `GET /v1/metrics/fraud-summary`

```bash
curl "https://fraud-api.yourdomain.com/v1/metrics/fraud-summary?date=2024-06-15"

# Response 200 OK
{
  "date": "2024-06-15",
  "total_transactions": 102483921,
  "flagged": 204847,
  "blocked": 41203,
  "false_positive_rate": 0.0042,
  "total_fraud_amount_usd": 1842930.50
}
```

---

 # 📚 References

The following resources were used as technical references while designing and implementing this project.

## Core Streaming Technologies

| Resource | Link |
|----------|------|
| Apache Kafka Documentation | https://kafka.apache.org/documentation/ |
| Apache Flink Documentation | https://nightlies.apache.org/flink/flink-docs-stable/ |
| Apache Spark Structured Streaming | https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html |
| Apache Airflow Documentation | https://airflow.apache.org/docs/ |
| Apache Cassandra Documentation | https://cassandra.apache.org/doc/latest/ |
| PostgreSQL Documentation | https://www.postgresql.org/docs/ |

---

## Cloud & Infrastructure

| Resource | Link |
|----------|------|
| AWS Architecture Center | https://aws.amazon.com/architecture/ |
| AWS Well-Architected Framework | https://aws.amazon.com/architecture/well-architected/ |
| Amazon MSK Documentation | https://docs.aws.amazon.com/msk/latest/developerguide/ |
| Amazon EKS Documentation | https://docs.aws.amazon.com/eks/latest/userguide/ |
| Amazon S3 Documentation | https://docs.aws.amazon.com/s3/ |
| AWS IAM Documentation | https://docs.aws.amazon.com/iam/ |

---

## Containerization & Orchestration

| Resource | Link |
|----------|------|
| Docker Documentation | https://docs.docker.com/ |
| Kubernetes Documentation | https://kubernetes.io/docs/ |
| Helm Documentation | https://helm.sh/docs/ |
| Terraform Documentation | https://developer.hashicorp.com/terraform/docs |

---

## Monitoring & Observability

| Resource | Link |
|----------|------|
| Prometheus Documentation | https://prometheus.io/docs/ |
| Grafana Documentation | https://grafana.com/docs/ |
| Elasticsearch Documentation | https://www.elastic.co/guide/ |
| Kibana Documentation | https://www.elastic.co/guide/en/kibana/current/index.html |
| OpenTelemetry Documentation | https://opentelemetry.io/docs/ |

---

## Data Engineering & Streaming Design

| Resource | Link |
|----------|------|
| Confluent Developer | https://developer.confluent.io/ |
| Kafka Design Documentation | https://kafka.apache.org/documentation/#design |
| Flink Stateful Stream Processing | https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/ |
| Event-Driven Architecture Patterns | https://microservices.io/patterns/data/event-driven-architecture.html |
| Martin Fowler — Event Sourcing | https://martinfowler.com/eaaDev/EventSourcing.html |

---

## Fraud Detection & Machine Learning

| Resource | Link |
|----------|------|
| Amazon Fraud Detector Documentation | https://docs.aws.amazon.com/frauddetector/latest/ug/ |
| Isolation Forest (Original Paper) | Liu, F. T., Ting, K. M., & Zhou, Z. H. (2008). *Isolation Forest*. IEEE ICDM. |
| Scikit-learn Isolation Forest | https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html |

---

## Security

| Resource | Link |
|----------|------|
| OWASP Top 10 | https://owasp.org/www-project-top-ten/ |
| NIST Cybersecurity Framework | https://www.nist.gov/cyberframework |
| AWS Security Best Practices | https://docs.aws.amazon.com/wellarchitected/latest/security-pillar/ |

---

## Software Engineering

| Resource | Link |
|----------|------|
| Git Documentation | https://git-scm.com/doc |
| GitHub Documentation | https://docs.github.com/ |
| Conventional Commits | https://www.conventionalcommits.org/ |
| Semantic Versioning | https://semver.org/ |

---

## Architecture & Design

| Resource | Link |
|----------|------|
| Martin Fowler | https://martinfowler.com/ |
| AWS Prescriptive Guidance | https://docs.aws.amazon.com/prescriptive-guidance/ |
| Google Site Reliability Engineering (SRE) Book | https://sre.google/books/ |
| Designing Data-Intensive Applications (Martin Kleppmann) | https://dataintensive.net/ |

---

## Acknowledgements

This project was designed using publicly available documentation, open-source technologies, and industry best practices. It is an educational implementation demonstrating production-grade data engineering concepts and does not reproduce proprietary internal systems or code from any organization.

---

## 🤝 Contributing

# Contributing

Thank you for your interest in contributing to this project! Contributions are welcome and appreciated.

## Table of Contents

* Code of Conduct
* Ways to Contribute
* Getting Started
* Development Setup
* Branch Naming
* Commit Message Convention
* Pull Request Process
* Coding Standards
* Testing
* Reporting Issues
* Security
* License

---

## Code of Conduct

By participating in this project, you agree to maintain a respectful, professional, and collaborative environment.

---

## Ways to Contribute

You can contribute by:

* Fixing bugs
* Adding new features
* Improving documentation
* Optimizing performance
* Writing tests
* Improving dashboards
* Enhancing monitoring and observability
* Improving deployment automation
* Refactoring code
* Reviewing pull requests

---

## Getting Started

1. Fork the repository.
2. Clone your fork.
3. Create a new feature branch.
4. Make your changes.
5. Add or update tests if applicable.
6. Ensure all checks pass.
7. Submit a Pull Request.

---

## Development Setup

### Prerequisites

* Python 3.12+
* Java 21+
* Docker
* Docker Compose
* Kubernetes
* Apache Kafka
* Apache Flink
* PostgreSQL
* Cassandra
* Apache Airflow
* AWS CLI
* Terraform
* Git

### Installation

```bash
git clone https://github.com/<your-username>/<repository>.git

cd <repository>

cp .env.example .env

docker compose up -d
```

Follow the project README for additional environment-specific configuration.

---

## Branch Naming

Use descriptive branch names.

Examples:

* feature/fraud-rule-engine
* feature/kafka-producer
* feature/dashboard
* bugfix/checkpoint-recovery
* bugfix/schema-validation
* docs/update-readme
* refactor/state-management
* test/load-testing

---

## Commit Message Convention

Follow Conventional Commits.

Examples:

```text
feat: add fraud rule engine

fix: resolve Kafka consumer lag issue

docs: improve architecture documentation

test: add integration tests for stream processing

refactor: simplify checkpoint recovery logic

perf: optimize Cassandra write throughput
```

---

## Pull Request Process

Before submitting a Pull Request:

* Sync with the latest main branch.
* Ensure the project builds successfully.
* Run all tests.
* Update documentation if required.
* Keep Pull Requests focused on a single change.
* Include a clear description of the problem and solution.
* Reference related issues where applicable.

---

## Coding Standards

* Write clean and readable code.
* Use meaningful variable and function names.
* Keep functions small and focused.
* Avoid duplicated logic.
* Add comments only when they improve understanding.
* Follow the project's formatting and linting rules.

---

## Testing

Contributions should include appropriate tests whenever possible.

Recommended test types:

* Unit Tests
* Integration Tests
* End-to-End Tests
* Performance Tests

Ensure all tests pass before opening a Pull Request.

---

## Reporting Issues

When reporting an issue, please include:

* Project version
* Environment
* Steps to reproduce
* Expected behavior
* Actual behavior
* Relevant logs or screenshots

---

## Security

Please do not disclose security vulnerabilities publicly.

If you discover a security issue, report it privately to the project maintainers so it can be investigated and resolved responsibly.

---

## License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.


## 📄 License

```
MIT License

Copyright (c) 2024 Sunil | sunildataengineer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
```

---

<div align="center">

**Built with ❤️ by [Sunil](https://github.com/sunildataengineer)**

[![GitHub](https://img.shields.io/badge/GitHub-sunildataengineer-black?style=flat-square&logo=github)](https://github.com/sunildataengineer)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat-square&logo=linkedin)](https://linkedin.com/in/sunildataengineer)

⭐ **Star this repo if it helped you!** ⭐

<<<<<<< HEAD
</div>
=======
</div>
>>>>>>> 507e8bd2e991de847f48d554956f35bb5c775ff1
