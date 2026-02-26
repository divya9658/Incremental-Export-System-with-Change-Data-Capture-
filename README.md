# CDC Incremental + Delta Export System

## 📌 Project Overview

This project implements a **Production-Ready Incremental Export System using Change Data Capture (CDC)** principles.

It enables downstream consumer systems to efficiently sync data by exporting only the changed records instead of the full dataset every time.

The system supports:

* Full Data Export (Initial Snapshot)
* Incremental Export (Updated Active Records)
* Delta Export (Insert, Update, Delete Tracking)

Exports are generated as CSV files for downstream consumption.

---

## 🚀 Key Features

* Full snapshot export
* Incremental export using watermark tracking
* Delta export with change detection
* Insert tracking
* Update tracking
* Soft delete tracking
* Consumer-specific watermark isolation
* Asynchronous background export jobs
* CSV file generation
* Indexed incremental queries
* Dockerized deployment
* Seeded dataset (100,000 users)

---

## 🏗️ System Architecture

Client System
↓
Export API Layer (Express)
↓
Watermark Tracking Logic
↓
PostgreSQL Database
↓
CSV Export Generator
↓
Output Directory

---

## 🛠️ Tech Stack

* Node.js
* Express.js
* PostgreSQL
* Docker
* CSV Writer

---

## 📦 Database Schema

### Users Table

| Column         | Type      | Description              |
| -------------- | --------- | ------------------------ |
| id             | BIGSERIAL | Primary Key              |
| name           | VARCHAR   | User name                |
| email          | VARCHAR   | Unique email             |
| created_at     | TIMESTAMP | Record creation time     |
| updated_at     | TIMESTAMP | Last updated time        |
| is_deleted     | BOOLEAN   | Soft delete flag         |
| operation_type | VARCHAR   | INSERT / UPDATE / DELETE |

---

### Watermarks Table

| Column           | Type      | Description                |
| ---------------- | --------- | -------------------------- |
| consumer_id      | VARCHAR   | Unique consumer identifier |
| last_exported_at | TIMESTAMP | Last sync timestamp        |
| updated_at       | TIMESTAMP | Watermark update timestamp |

---

## 🔁 Export Types

### 1️⃣ Full Export

* Exports all active records
* Initializes consumer watermark
* Generates CSV snapshot

Endpoint:

```
POST /exports/full
```

---

### 2️⃣ Incremental Export

* Uses watermark tracking
* Exports only updated active records
* Skips deleted records

Endpoint:

```
POST /exports/incremental
```

---

### 3️⃣ Delta Export

* Detects INSERT operations
* Detects UPDATE operations
* Detects DELETE operations
* Exports all changed records

Endpoint:

```
POST /exports/delta
```

---

## 🐳 Running the Project with Docker

### Step 1: Build and Start Containers

```
docker-compose up --build
```

---

## 🧪 Testing CDC Flow

### 1. Run Full Export

```
POST http://localhost:8080/exports/full
Header: X-Consumer-ID: consumer-1
```

---

### 2. Update a Record

```
UPDATE users
SET name = 'Updated User', updated_at = NOW()
WHERE id = 10;
```

---

### 3. Soft Delete a Record

```
DELETE http://localhost:8080/users/5
```

---

### 4. Run Delta Export

```
POST http://localhost:8080/exports/delta
Header: X-Consumer-ID: consumer-1
```

---

## 📂 Output

All generated CSV files are stored in:

```
/output
```

Example:

```
full_consumer-1_XXXXXXXX.csv
incremental_consumer-1_XXXXXXXX.csv
delta_consumer-1_XXXXXXXX.csv
```

---

## 📈 Future Enhancements

* Change log table
* Background job queue
* Retry mechanism
* Job status tracking
* Cloud storage integration
* Streaming CSV exports

---

## 🎓 Conclusion

This project demonstrates a watermark-based Incremental Export System extended with Delta CDC capabilities for efficient downstream data synchronization.

It simulates production-style change data tracking in a containerized environment.
