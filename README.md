# Optimizing Patient Care: A Hospital Readmissions Data Warehouse

> Building an end-to-end data warehouse to track, analyze, and ultimately help reduce hospital readmissions — from raw patient data all the way to interactive Tableau dashboards.

---

## What This Project Is About

Hospital readmissions are one of those problems that hits healthcare from every angle — they drive up costs, strain resources, and most importantly, they signal that something went wrong in a patient's care. This project builds a full data warehousing pipeline designed to collect patient data, model it for analytics, and surface the kinds of insights that can actually help administrators and clinicians make better decisions.

We started with two real-world datasets, designed and normalized a relational database schema in PostgreSQL, built out a star schema data warehouse, ran ETL workflows in Talend, and connected it all to Tableau for interactive OLAP-style analysis.

The whole thing was built as a group project for IE 6750 (Data Analytics Engineering), and it covers the full lifecycle of a data warehousing project — from problem definition and schema design through to ETL orchestration and business intelligence dashboards.

---

## The Problem

Unplanned hospital readmissions cost the U.S. healthcare system billions of dollars annually and can trigger penalties under Medicare regulations. But beyond the money, high readmission rates often point to gaps in treatment quality, discharge planning, or follow-up care.

This project asks: **what if we could build a system that tracks the right data, models it intelligently, and lets healthcare teams spot the patterns behind readmissions before they become trends?**

---

## Data Sources

We pulled from two external datasets and supplemented them with synthetic data where needed:

**1. Kaggle — Hospital Readmissions Dataset**
[https://www.kaggle.com/datasets/dubradave/hospital-readmissions](https://www.kaggle.com/datasets/dubradave/hospital-readmissions)

This is the backbone of the project. It contains ~25,000 patient records with attributes like age bracket, time in hospital, number of procedures, medications, lab work, diagnoses, and whether the patient was readmitted. Originally derived from a diabetes-focused clinical study.

**2. Centers for Medicare & Medicaid Services (CMS)**
[https://data.cms.gov/provider-data/dataset/9n3s-kdb3](https://data.cms.gov/provider-data/dataset/9n3s-kdb3)

Used to supplement the analysis with real-world provider and facility-level data.

**3. Synthetic Data (via Python's Faker library)**

Since the Kaggle dataset doesn't include things like patient names, contact info, insurance providers, or staff schedules, we generated realistic synthetic data using Faker to populate the operational database's static reference tables.

---

## Architecture Overview

The project follows a classic data warehousing architecture:

```
[ Raw Data Sources ]
        │
        ▼
[ Operational Database (PostgreSQL) ]
   - 16 normalized tables (3NF)
   - Static reference + transactional data
        │
        ▼
[ ETL Pipeline (Talend Open Studio) ]
   - Data cleaning (null handling, deduplication)
   - SCD Type 1 & Type 2 transformations
   - Aggregations (avg cost by diagnosis, avg satisfaction by provider)
   - Complete + incremental loads
   - Control table for job tracking
        │
        ▼
[ Data Warehouse (PostgreSQL - Star Schema) ]
   - Fact table: ReadmissionFacts
   - Dimensions: Patient, Time, Provider, Department, Diagnosis, Insurance, Geographic, Procedure
        │
        ▼
[ Analytics & Visualization (Tableau) ]
   - OLAP operations: roll-up, drill-down, slice, dice, pivot, drill-across
   - KPI dashboards for readmission trends, costs, demographics, provider performance
```

---

## Database Design

### Operational Database (OLTP)

The operational database has **16 tables** organized into static reference data and transactional data, all normalized to 3NF:

**Static Reference Tables:** Patients, Healthcare Providers, Hospital Departments, Medications, Insurance Providers, Medical Codes

**Transactional Tables:** Admissions, Discharges, Medical Procedures, Medications Administered, Laboratory Tests, Readmission Records, Billing Records, Patient Feedback, Staff Scheduling, Equipment Usage Logs

Every table has proper primary keys, and foreign key constraints enforce referential integrity across the schema. For example, each admission links back to a patient, a department, and an attending provider.

### Data Warehouse (OLAP — Star Schema)

The warehouse is structured around a central **ReadmissionFacts** fact table with the following measures: total charges, length of stay, satisfaction score, associated costs, and readmission rate.

**Dimension tables** include:
- **PatientDimension** — age group, gender, ethnicity, insurance provider
- **TimeDimension** — day, month, quarter, year (supports full temporal drill-down)
- **ProviderDimension** — specialty, department affiliation
- **DepartmentDimension** — department name, location
- **DiagnosisDimension** — ICD codes and descriptions
- **InsuranceDimension** — provider name, contact info
- **GeographicDimension** — city, state, region (hierarchical)
- **ProcedureDimension** — procedure codes and descriptions

---

## ETL Pipeline

The ETL process was built in **Talend Open Studio for Data Integration** and handles the full flow from operational database to warehouse:

**Extraction:** Data is pulled from the PostgreSQL operational database and supplemented with CSV files for incremental updates.

**Transformation:**
- Null cleaning across admissions, healthcare providers, geographic dimensions, and medical procedures
- Standardization of the patient readmissions fact table (date formatting, key mapping)
- **SCD Type 1** transformations for non-historical attributes (e.g., patient contact info — overwrite)
- **SCD Type 2** transformations for attributes that need history tracking (e.g., insurance provider changes — new versioned records)
- **Aggregations** computed via Talend: average cost per diagnosis and average satisfaction score per provider

**Loading:**
- **Complete loads** for initial warehouse setup or full refreshes
- **Incremental loads** for ongoing updates using timestamp-based change detection
- A **control table** (`data_warehouse.control_table`) logs every ETL job with the job name, last load timestamp, and row count for auditability

A **wrapper job** orchestrates all sub-jobs in the correct sequence, ensuring dependencies are respected.

---

## Data Population (Python)

The `final_import_code.py` script handles populating the operational database. It uses `psycopg2` for PostgreSQL connections and `pandas` + `Faker` for data generation and insertion.

Here's what it does, in order:
1. Generates 10 synthetic insurance providers
2. Inserts 6 hospital departments (Emergency, Cardiology, Neurology, Orthopedics, Oncology, Pediatrics) plus additional specialties
3. Creates 20 synthetic healthcare providers mapped to departments
4. Loads 6 common medications with real brand/generic names
5. Inserts ICD diagnosis and procedure codes
6. Loads the Kaggle dataset and generates patient records with estimated DOBs, synthetic demographics, and random insurance assignments
7. Creates admission records linked to patients, departments, and providers
8. Generates discharge records, medical procedures, medications administered, lab tests, readmission records, billing records, patient feedback, staff schedules, and equipment entries

---

## Tableau Dashboards

The Tableau workbook (`GROUP_B_ON_PREM_TABLEAU.twbx`) connects directly to the PostgreSQL data warehouse and implements several OLAP-style views:

- **Readmission Trends** — 30-day readmission rates broken down by temporal hierarchy (day → month → quarter → year)
- **Cost Trends** — total charges over time with drill-down capability
- **Cost Analysis** — associated costs broken down by diagnosis type
- **Demographics Analysis** — readmission counts segmented by age group
- **Department/Specialty Drill-Down** — readmission rates at the department and specialty level
- **Dice (Age × Insurance)** — cross-dimensional filtering by age group and insurance provider
- **Slice (Filter by Diagnosis)** — isolating readmissions for specific conditions
- **Pivot (Gender × Age)** — readmission rates by demographic intersection
- **Drill-Across (Provider Comparison)** — side-by-side provider performance with filters for specialty, diagnosis, and month
- **Roll-Up** — daily admissions and charges aggregated upward through the time hierarchy

### Key Insights from the Analysis

- Readmission costs scale directly with readmission volume — not a surprise, but the data makes the case clearly.
- The **Cardiology department** was responsible for a disproportionate share of readmissions, flagging it for a potential operational audit.
- **Older patients** (71-80 and 81-90 age brackets) had significantly higher readmission rates, suggesting a need for more intensive discharge planning and follow-up for these groups.
- **Circulatory disease** was the most common diagnosis associated with readmissions by a wide margin.

---

## Tech Stack

- **PostgreSQL** — operational database + data warehouse
- **Python** (psycopg2, pandas, Faker) — data generation and database population
- **Talend Open Studio** — ETL pipeline design and execution
- **Tableau** — OLAP dashboards and visualization

---

## How to Run This

### Prerequisites

- PostgreSQL 14+
- Python 3.8+ with `psycopg2`, `pandas`, `faker`, and `openpyxl`
- Talend Open Studio for Data Integration (optional — for ETL workflow modification)
- Tableau Desktop or Tableau Public (for dashboard exploration)

### Steps

1. **Clone the repo**
   ```bash
   git clone https://github.com/ateraalam/data-warehousing.git
   cd data-warehousing
   ```

2. **Set up the PostgreSQL databases** — run the SQL scripts in `sql/` to create the operational database schema and the data warehouse schema.

3. **Download the Kaggle dataset** from [here](https://www.kaggle.com/datasets/dubradave/hospital-readmissions) and place it in the `data/` directory.

4. **Run the data population script**
   ```bash
   pip install psycopg2 pandas faker openpyxl
   python scripts/final_import_code.py
   ```
   > **Note:** You'll need to update the database connection parameters and the CSV file path in the script to match your local setup.

5. **Run the Talend ETL jobs** 

6. **Open the Tableau workbook** (`tableau/GROUP_B_ON_PREM_TABLEAU.twbx`) and point it at your local PostgreSQL data warehouse.

---

## Repo Structure

```
data-warehousing/
├── README.md
├── scripts/
│   └── final_import_code.py               # Python script for DB population
│   └── dataframes.py                      # Create dataframes on postgresql  
├── tableau/
│   └── GROUP_B_ON_PREM_TABLEAU.twbx       # Tableau workbook with dashboards
├── docs/
│   └── Final_Report_Project_1.pdf         # Full project report (all milestones)
└── diagrams/
    ├── erd.png                            # Entity-Relationship Diagram

```

---

## Project Milestones

This project was developed across four milestones:

**Milestone 1** — Problem definition, dataset selection, objectives, and stakeholder analysis. Identified hospital readmissions as the core problem and defined KPIs for tracking.

**Milestone 2** — ![ERD](diagrams/erd.png) design, relational model mapping, normalization to 3NF, and PostgreSQL implementation. Defined 16 tables with proper constraints and identified dimensions, hierarchies, and measures for the future warehouse.

**Milestone 3** — Designed the star schema data warehouse with a ReadmissionFacts table and 8 dimension tables. Defined OLAP operations (roll-up, drill-down, slice, dice, pivot, drill-across, rename) and implemented the logical model in PostgreSQL.

**Milestone 4** — Built the full ETL pipeline in Talend with complete/incremental loads, SCD Type 1 & 2 transformations, data aggregations, control flow logging, and a wrapper job for orchestration. Created Tableau dashboards implementing all OLAP operations with actionable insights.

---

## Limitations & Future Work

- The synthetic data (Faker-generated names, addresses, etc.) means the demographic analysis won't reflect real-world distributions perfectly.
- The Cardiology department handling all readmissions is partly an artifact of how the data was mapped — a more granular specialty-to-department mapping would improve this.
- Currently only two aggregations are computed in the ETL pipeline. Adding more (e.g., readmission rate by insurance provider, average length of stay by department) would enrich the dashboards.
- Future iterations could incorporate **predictive modeling** (logistic regression, decision trees) to flag high-risk patients at admission time.
- Integrating **real-time streaming** data via tools like Apache Kafka could turn this from a batch-oriented warehouse into a near-real-time analytics platform.

---

## Authors

**Atera Alam** & **Priska Mohunsingh** — [GitHub](https://github.com/ateraalam)

*Built for IE 6750 — Data Analytics Engineering. If you have questions or feedback, feel free to open an issue!*
