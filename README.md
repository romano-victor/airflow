🧰 Airflow POC — First-Time Setup Guide

Welcome!
This guide will walk you through everything you need to do to run the Airflow POC locally for the first time. Follow each step carefully — once your environment is set up, daily usage becomes very simple.

📦 Requirements

Before starting, make sure you have:

Docker Desktop installed and running

Docker Compose (comes with Docker Desktop)

Git

(Optional) Python + Poetry if you want to work on the app

🚀 First-Time Setup

1️⃣ Clone the repository
git clone <repo-url>
cd airflow_poc

2️⃣ Build all containers

This downloads Airflow, Postgres, LocalStack, and builds the app image.

docker compose build

3️⃣ Start the services

This launches Postgres, LocalStack, the Airflow Webserver + Scheduler, and the app container (if enabled).

docker compose up -d

4️⃣ Initialize the Airflow database (FIRST TIME ONLY)

Airflow will not start correctly until its metadata DB is created.

Run:

docker compose run airflow-webserver airflow db init

docker compose run airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

What this does:

Creates Airflow’s metadata tables in Postgres

Prepares the environment for the scheduler and webserver

5️⃣ Restart Airflow so everything picks up the DB
docker compose up -d


At this point, all services should be healthy.

🎛️ Accessing the Airflow UI

Once everything is running:

👉 http://localhost:8080

Default credentials:

Username: admin

Password: admin

If you see errors like “Airflow not initialized,” it means Step 4 was skipped.

📁 Project Layout
airflow_poc/
├── app/                 # Optional Python service
├── dags/                # Your Airflow DAGs
├── Dockerfile           # App Dockerfile
├── docker-compose.yml   # Main orchestrator
├── localstack/          # LocalStack data
├── pyproject.toml       # Python dependencies (Poetry)
├── poetry.lock
└── README.md
