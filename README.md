# NBA Feature Store Pipeline

## Overview

This project implements a **production-style data pipeline** that ingests NBA player game statistics from the NBA Stats API and stores them in a **partitioned BigQuery feature store**.

The system pulls data from multiple NBA API endpoints, merges player-level statistics, performs validation checks, and loads the results into a structured warehouse designed for analytics and modeling.

This repository represents **Phase 1: Data Infrastructure**, which builds the core feature store used for downstream modeling and analytics.

---

## Architecture

```
NBA Stats API
      │
      ▼
Endpoint Pull Layer
(traditional / advanced / usage / tracking)
      │
      ▼
Merge + Feature Engineering
      │
      ▼
Validation Layer
      │
      ▼
BigQuery Feature Store
(pr_see_daily_player_game_log)
      │
      ▼
Monitoring & Integrity Audits
```

---

## Key Features

- Multi-endpoint NBA API ingestion
- Retry-protected API calls with exponential backoff
- Adaptive rate limiting and session management
- Partitioned BigQuery warehouse design
- Schema-locked feature store
- Data validation safeguards
- Idempotent ingestion architecture
- Monitoring and pipeline health audits

---

## Feature Store Table

```
pr_see_daily_player_game_log
```

### Partitioning
```
GAME_DATE
```

### Clustering
```
PLAYER_ID
TEAM_ID
```

Partition filtering is enforced to prevent accidental full-table scans and control BigQuery query costs.

---

## Data Sources

NBA Stats API endpoints used:

- `boxscoretraditionalv3`
- `boxscoreadvancedv3`
- `boxscoreusagev3`
- `boxscoreplayertrackv3`
- `boxscorefourfactorsv3`
- `boxscoresummaryv3`
- `scoreboardv3`
- `commonteamroster`

These endpoints are merged to produce a **comprehensive player game feature set**.

---

## Technology Stack

Python  
NBA API (`nba_api`)  
Google BigQuery  
Pandas  
Requests  

---

## Pipeline Capabilities

### Reliable Data Ingestion
The pipeline implements retry logic with exponential backoff to protect against API instability and transient failures.

### Rate Limit Protection
An adaptive rate governor dynamically adjusts request pacing to prevent NBA Stats API throttling.

### Data Validation
Before ingestion, the pipeline validates:

- duplicate row keys
- missing player IDs
- corrupted merges
- negative minutes
- empty dataframes

### Atomic Day-Level Ingestion
Each game date is processed as a complete unit. If any game fails, the entire day is aborted to prevent partial ingestion.

---

## Monitoring & Data Integrity

The project includes several monitoring tools:

### Data Health Audit
Checks for:

- missing regular season dates
- duplicate row keys
- abnormal row counts

### Game Integrity Audit
Validates:

- games contain two teams
- reasonable player counts
- absence of corrupted rows

### Feature Store Command Center
Provides a quick operational overview including:

- ingestion freshness
- total rows
- total games
- unique players
- partition counts

---

## Project Structure

```
nba-feature-store
│
├── main.py
├── config.py
├── schema.py
│
├── ingestion
│   ├── ingestion_engine.py
│   └── pull_games.py
│
├── utils
│   ├── retry.py
│   ├── validation.py
│   └── logging.py
│
├── requirements.txt
├── README.md
│
└── notebook_prototype
    └── feature_store_pipeline.ipynb
```

The notebook prototype contains the original development environment used to design the pipeline before converting it into a structured Python project.

---

## Future Work (Phase 2)

Phase 2 will build analytical layers on top of the feature store, including:

- player performance modeling
- projection systems
- feature engineering pipelines
- predictive analytics workflows

---

## Author

ZH