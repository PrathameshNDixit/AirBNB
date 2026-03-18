# 🏠 Airbnb Data Transformation Pipeline (dbt + Snowflake)

## 📋 Project Overview
This project implements a complete **Medallion Architecture** (Bronze, Silver, Gold) to transform raw Airbnb data into an analytics-ready Star Schema within **Snowflake**. Using **dbt**, I’ve built a pipeline that prioritizes scalability through metadata-driven models and historical integrity through SCD Type 2 snapshots.

---

## 🛠️ Tech Stack
* **Data Warehouse:** Snowflake
* **Transformation Engine:** dbt (Core)
* **Language:** SQL (Jinja2)
* **Workflow:** Git (Feature-branching)
* **Environment:** VS Code with dbt Power User

---

## 🏗️ Data Architecture

### 1. Bronze Layer (Staging)
* Acts as the landing zone for raw CSV data.
* Implements basic casting and column renaming to ensure a consistent foundation.

### 2. Silver Layer (Cleanse & Standardize)
* **Standardization:** Applied `snake_case` naming and handled inconsistent strings (e.g., `host_name_cleaned`).
* **Materialization:** Used **Incremental** models with `unique_key` constraints to optimize Snowflake warehouse costs by only processing new or updated records.

### 3. Gold Layer (Analytics & Snapshots)
* **Dimensions:** Built SCD Type 2 Snapshots (`dim_hosts_snapshot`) to track historical changes in host attributes like `is_superhost` status.
* **Fact Table:** A central `fact_bookings` table that serves as the "Source of Truth" for all booking transactions.
* **OBT (One Big Table):** A high-performance, denormalized table built using a **metadata-driven Jinja approach**, allowing the schema to scale dynamically.

---

## 🚀 Key Technical Highlights

### **1. Metadata-Driven Orchestration**
Instead of hard-coding 50+ columns in the final Gold layer, I implemented a configuration-driven approach. By defining table metadata in a Jinja list, the `obt.sql` model dynamically generates the `SELECT` and `JOIN` logic, significantly reducing technical debt and manual errors.

### **2. SCD Type 2 & Point-in-Time Joins**
To ensure reporting accuracy, the pipeline uses **Point-in-Time joins**. When joining bookings to the host dimension, the logic checks the `booking_date` against the `dbt_valid_from` and `dbt_valid_to` timestamps, ensuring we report the host's status *at the time of the booking*, not just their current status.

### **3. Data Quality & Governance**
* **Schema Tests:** Implemented `unique` and `not_null` tests on all primary keys.
* **Custom Logic Tests:** Added checks to ensure `total_booking_amount` remains positive and valid across the pipeline.

---

## 🚦 Getting Started

1.  **Install dependencies:**
    ```bash
    dbt deps
    ```
2.  **Run the Snapshots (Historical Tracking):**
    ```bash
    dbt snapshot
    ```
3.  **Execute the Pipeline:**
    ```bash
    dbt run
    ```
4.  **Validate Data Quality:**
    ```bash
    dbt test
    ```

---

## 📈 Future Roadmap
* **CI/CD:** Integrate GitHub Actions to automate `dbt test` on every Pull Request.
* **Orchestration:** Deploying to **Airflow** for scheduled production runs.
* **Observability:** Adding **Elementary** or **dbt-artifacts** for advanced pipeline monitoring.

---
**Author:** Prathamesh Dixit  
**Role:** Data Engineer (3+ Years Experience)