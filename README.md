# Enterprise Healthcare Claims Lakehouse Platform on Azure

An end-to-end Azure healthcare data platform that supports batch ingestion, real-time adjudication updates, medallion-based transformations, warehouse modeling, anomaly detection, and business dashboards.

## Architecture Overview

This platform implements a modern lakehouse architecture using Azure services:

- **Azure Data Factory**: Batch orchestration and pipeline management
- **Azure Event Hubs**: Real-time claim adjudication event streaming
- **Azure Data Lake Storage Gen2**: Raw and curated data storage
- **Azure Databricks**: PySpark batch and streaming transformations
- **Delta Lake**: ACID transactions for Bronze/Silver/Gold layers
- **Azure Synapse Analytics**: SQL serving layer for analytics
- **dbt**: Analytics engineering and warehouse transformations
- **Power BI**: Business intelligence dashboards
- **Azure Key Vault**: Secure secrets management
- **Azure Monitor / Log Analytics**: Observability and monitoring

## Medallion Architecture

### Bronze Layer (Raw)
- Append-only tables from source systems
- Minimal validation, preserve original format
- Includes metadata: ingestion_timestamp, source_file_name, pipeline_run_id

### Silver Layer (Validated)
- Cleaned, standardized, and validated data
- Data quality checks and quarantine handling
- CDC/MERGE logic for claim status updates
- Referential integrity enforcement

### Gold Layer (Business-Ready)
- Fact and dimension tables
- Business marts for analytics
- Optimized for reporting and BI

## Key Features

✅ **Batch Ingestion**: Daily claims, members, providers, and reference data  
✅ **Real-time Streaming**: Claim adjudication status updates via Event Hubs  
✅ **Data Quality**: Automated validation, quarantine, and monitoring  
✅ **CDC Processing**: MERGE operations for claim status changes  
✅ **Warehouse Modeling**: Dimensional modeling with dbt on Synapse  
✅ **Anomaly Detection**: Duplicate claims, outlier amounts, suspicious patterns  
✅ **Executive Dashboards**: Power BI with provider performance, denial trends, utilization  
✅ **Audit & Observability**: Complete pipeline audit trail and monitoring  

## Project Structure

```
enterprise-healthcare-claims-lakehouse-azure/
├── README.md
├── architecture/
│   ├── azure_architecture.png
│   ├── medallion_model.png
│   ├── synapse_dbt_flow.png
│   └── dashboard_mockup.png
│
├── data/
│   ├── raw/
│   │   ├── claims_raw.csv
│   │   ├── members_raw.csv
│   │   ├── providers_raw.csv
│   │   ├── diagnosis_ref.csv
│   │   ├── procedure_ref.csv
│   │   └── payer_ref.csv
│   └── sample_events/
│       └── claim_status_events.json
│
├── notebooks/
│   ├── 01_bronze_claims_ingestion.py
│   ├── 02_bronze_members_ingestion.py
│   ├── 03_bronze_providers_ingestion.py
│   ├── 04_bronze_reference_ingestion.py
│   ├── 05_silver_claims_transform.py
│   ├── 06_silver_members_transform.py
│   ├── 07_silver_providers_transform.py
│   ├── 08_claim_status_merge.py
│   ├── 09_data_quality_runner.py
│   ├── 10_build_gold_dimensions.py
│   ├── 11_build_gold_fact_claims.py
│   ├── 12_build_gold_marts.py
│   ├── 13_publish_gold_to_synapse.py
│   ├── 14_audit_metrics_writer.py
│   └── stream_claim_status_events.py
│
├── src/
│   ├── config/
│   │   └── config.py
│   ├── quality/
│   │   ├── rules.py
│   │   └── runner.py
│   ├── utils/
│   │   ├── spark_session.py
│   │   ├── audit.py
│   │   └── helpers.py
│   └── monitoring/
│       └── alerts.py
│
├── adf/
│   ├── pl_ingest_healthcare_batch_files.json
│   ├── pl_run_databricks_medallion_jobs.json
│   └── pl_pipeline_audit_monitoring.json
│
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── dimensions/
│   │   └── marts/
│   └── tests/
│
├── sql/
│   ├── synapse_schema.sql
│   ├── analytics_queries.sql
│   └── dashboard_queries.sql
│
├── dashboard/
│   ├── kpi_definitions.md
│   └── powerbi_layout.md
│
├── tests/
│   ├── test_quality_rules.py
│   ├── test_transformations.py
│   └── test_gold_outputs.py
│
├── requirements.txt
├── Dockerfile
└── .github/workflows/ci.yml
```

## Data Flow

1. **Batch Ingestion**: ADF orchestrates daily file ingestion from source systems to ADLS raw zone
2. **Bronze Processing**: Databricks loads raw files into Bronze Delta tables with metadata
3. **Silver Transformations**: Data cleaning, validation, deduplication, and quality checks
4. **Streaming Updates**: Event Hubs captures claim adjudication updates, processed by Structured Streaming
5. **Gold Modeling**: Business-ready fact and dimension tables, analytical marts
6. **Warehouse Publishing**: Gold tables published to Synapse for enterprise access
7. **dbt Transformations**: Analytics engineering layer with tests and documentation
8. **BI Dashboarding**: Power BI connects to Synapse for executive reporting

## Core Tables

### Bronze Tables
- `bronze_claims`
- `bronze_members` 
- `bronze_providers`
- `bronze_diagnosis_ref`
- `bronze_procedure_ref`
- `bronze_payer_ref`
- `bronze_claim_status_events`

### Silver Tables
- `silver_claims_current`
- `silver_claims_history`
- `silver_members`
- `silver_providers`
- `silver_quarantine_records`
- `silver_data_quality_results`

### Gold Tables
- `gold_fact_claims`
- `gold_dim_member`
- `gold_dim_provider`
- `gold_dim_diagnosis`
- `gold_dim_procedure`
- `gold_dim_payer`
- `gold_mart_provider_performance`
- `gold_mart_denial_trends`
- `gold_mart_member_utilization`
- `gold_mart_claim_anomalies`

## Key Business Metrics

### Provider Performance
- Total claims by provider
- Average claim amount
- Denial rate by provider
- Specialty-wise utilization

### Denial Trends
- Denied claims by month/reason
- Denied amount by payer type
- Providers with high denial rates

### Member Utilization
- Claims by member/payer
- High-cost members
- Diagnosis frequency patterns

### Anomaly Detection
- Suspicious duplicate claims
- Outlier claim amounts
- Unusual provider behavior patterns

## Getting Started

### Prerequisites
- Azure subscription with appropriate services provisioned
- Databricks workspace with Delta Lake support
- Azure Data Factory instance
- Azure Synapse Analytics workspace
- Power BI service

### Installation
1. Clone this repository
2. Configure Azure service connections in `src/config/config.py`
3. Set up ADLS Gen2 container structure
4. Deploy ADF pipelines from `adf/` folder
5. Configure dbt profile for Synapse connection
6. Run notebooks in sequence from `notebooks/` folder

### Quick Start
```bash
# Install Python dependencies
pip install -r requirements.txt

# Run batch ingestion pipeline
databricks jobs run --job-id batch-ingestion-pipeline

# Start streaming processing
databricks jobs run --job-id streaming-claim-status

# Run dbt transformations
dbt run --profiles-dir profiles/

# Deploy Power BI dashboard
# Import .pbix from dashboard/ folder to Power BI service
```

## Monitoring & Observability

- **Pipeline Metrics**: Row counts, processing times, error rates
- **Data Quality**: Validation results, quarantine volumes
- **System Health**: Resource utilization, job success/failure rates
- **Business Metrics**: Claim volumes, denial rates, provider performance

## Security & Compliance

- Data encryption at rest and in transit
- Role-based access control (RBAC)
- Audit logging for all data transformations
- HIPAA-compliant data handling practices
- PII detection and masking capabilities
