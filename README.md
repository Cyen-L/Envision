# Envision – ClickHouse Data Pipeline

This project provides a complete data ingestion and aggregation pipeline for transactional data using ClickHouse, designed for OLAP workloads. It includes scripts for incremental ingestion, pre‑aggregation, and analytical queries.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Installation Steps](#installation-steps)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [Ingestion Pipeline](#ingestion-pipeline)
- [Pre‑aggregation Tables](#pre-aggregation-tables)
- [Analytics Queries](#analytics-queries)
- [Usage Notes](#usage-notes)

---

## Project Structure
.
├── docker-compose.yml # ClickHouse service definition
├── schema.sql # Table schema for raw transactions
├── requirements.txt # Python dependencies
├── utils.py # Shared utilities (config, client, helpers)
├── ingest.py # JSON ingestion script
├── ingestion_tracking.txt # Tracks already processed files
├── aggregate.py # Builds pre‑aggregation tables
├── C1.py # Daily total transaction count
├── C2.py # Daily total transfer amount
├── C3.py # Daily count by currency
├── C4.py # Top 20 usernames by amount (date range)
├── Bonus-1.py # Daily username count by site ID (date range)
├── config.json # Connection & runtime configuration
└── json_files/ # Input directory for JSON files


---

## Setup

### Prerequisites

- Python 3.13+
- Docker & Docker Compose
- Git
- (Optional) Visual Studio Code

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/Cyen-L/Envision.git
   cd Envision

2. **Download the sample dataset**
   ```bash
   wget https://big-data-interview.s3.ap-southeast-2.amazonaws.com/sample_data.zip

3. **Create input directory and extract data**
   ```bash
   mkdir json_files
   unzip sample_data.zip -d json_files

4. **Set up Python virtual environment**
   ```bash
   python -m venv env
   source env/bin/activate 

5. **Install dependencies**
   ```bash
   pip install -r requirements.txt

6. **Start ClickHouse with Docker Compose**
   ```bash
   docker-compose up -d

7. **Ingest JSON files**
   ```bash
   python ingest.py

8. **Build pre‑aggregation tables**
   ```bash
   python aggregate.py

9. **Explore data via ClickHouse Play**
   Open [ClickHouse Play](http://localhost:8123/play) in your browser.

---

## Configuration
All connection parameters are defined in `CONFIG.JSON`, these values can be modified as needed:
```json
{
  "host": "localhost",
  "port": 9000,
  "database": "olap_db",
  "user": "admin",
  "password": "admin"
}

---

## Database Schema