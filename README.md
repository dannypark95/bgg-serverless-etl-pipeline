# BGG AI-Powered ETL Pipeline 🎲🤖

A high-performance, serverless ETL (Extract, Transform, Load) platform built on **Google Cloud Platform (GCP)**. This pipeline automates the ingestion, processing, and semantic localization of BoardGameGeek (BGG) metadata into a high-fidelity Firestore database.

## 🏗️ Architecture & Methodology
The pipeline follows a decoupled, sequential **DAG (Directed Acyclic Graph)** architecture to ensure high data integrity, low operational cost, and scalability.

### 1. Extraction & Pre-Processing (`bgg_csv.py`)
* **Objective**: Ingest raw BGG datasets and identify base-expansion relationships.
* **Logic**: Filters massive dumps for highly-rated base games and queries the BGG API to map expansion links, creating a unified sync "Master List" stored in **Cloud Storage**.

### 2. Intelligent Data Sync (`bgg_extractor.py`)
* **Objective**: Populate Firestore with rich metadata while minimizing write costs.
* **Methodology**: 
    * **State Persistence**: Utilizes a persistent SQLite cache and **MD5 hashing** to detect delta changes. 
    * **Cost Optimization**: Only writes to Firestore if the underlying BGG data has physically changed, reducing billable database operations by up to **90%**.

## 🛠️ Infrastructure & Tech Stack
* **Orchestration**: **Google Cloud Workflows** (Serverless DAG).
* **Compute**: **Cloud Run Jobs** (Stateless Docker containers).
* **Database**: **Firestore** (NoSQL) for game data & **SQLite** for persistent sync cache.
* **CI/CD**: **Google Cloud Build (2nd Gen)** for automated, regionalized deployments.

## 🚀 Deployment & CI/CD
Pushing to the `main` branch triggers the **Cloud Build** pipeline:
1. **Docker Build**: Packages Python environment and dependencies from the `Dockerfile`.
2. **Registry Push**: Images are stored in the Google Container Registry (GCR).
3. **Automated Deployment**: Deploys three separate Cloud Run Jobs (`bgg-csv-job`, `bgg-extractor-job`, `boardlife-scraper-job`).

## 📅 Roadmap & Future Enhancements
* **Data Observability Dashboard**: Integration with **Looker Studio** to monitor pipeline health, sync success rates, and game metadata trends.
* **Infrastructure as Code (IaC)**: Migrating manual cloud setup to **Terraform** for reproducible regional deployments.
* **Advanced AI Logic**: Implementing reasoning models to automatically categorize games into complex thematic genres beyond standard BGG categories.

## 📄 License & Credits
* **Data Source**: BoardGameGeek XML API 2.0.
* **License**: MIT.
* **Author**: Daniel Park.