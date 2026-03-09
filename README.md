# BGG Serverless ETL Pipeline 🎲☁️

An automated, serverless ETL (Extract, Transform, Load) platform built on **Google Cloud** to ingest, process, and Semantically localize BoardGameGeek (BGG) metadata.

---

## 🏗️ Architecture & Methodology
The pipeline uses a sequential **DAG (Directed Acyclic Graph)** orchestrated via Google Cloud Workflows to ensure data integrity and cost efficiency.

### 1. Data Ingestion (`bgg_csv.py`)
* Processes raw BGG datasets to identify base-expansion relationships.
* Generates a sync "Master List" stored in **Cloud Storage**.

### 2. Intelligent Sync (`bgg_extractor.py`)
* Fetches rich metadata (mechanics, ratings, images) from BGG XML API 2.0.
* **Cost Optimization**: Uses a persistent SQLite cache and **MD5 hashing** to trigger Firestore writes only when data changes occur, reducing billable ops by ~90%.

### 3. AI Localization (`ai_localizer.py`)
* Replaces brittle scrapers with an **LLM-based engine** for semantic translation.
* Handles phonetic transliteration and official regional retail naming for global markets.

---

## 🛠️ Tech Stack
* **Orchestration**: Google Cloud Workflows.
* **Compute**: Cloud Run Jobs (Dockerized Python 3.11).
* **Database**: Firestore (NoSQL).
* **CI/CD**: Google Cloud Build (2nd Gen) with Secret Manager integration.

---

## 🚀 Deployment & CI/CD
Pushing to the `main` branch triggers an automated build:
1. **Docker Build**: Packages Python environment and dependencies.
2. **Registry Push**: Images are stored in Google Container Registry (GCR).
3. **Automated Deployment**: Deploys three independent Cloud Run Jobs.

---

## 📅 Roadmap
* **Observability**: Integration with **Looker Studio** for pipeline health monitoring.
* **Infrastructure as Code (IaC)**: Migration to **Terraform** for reproducible deployments.
* **Advanced AI**: Reasoning models for thematic genre classification beyond standard tags.

---

## 📄 Credits
* **Data Source**: BoardGameGeek XML API 2.0.
* **Author**: Daniel Park.