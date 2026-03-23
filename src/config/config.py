"""
Configuration settings for the Enterprise Healthcare Claims Lakehouse Platform.
"""

from typing import Dict, Any
import os

class Config:
    """Configuration class for Azure services and data paths."""
    
    # Azure Storage Configuration
    ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME", "healthcarelakehouse")
    ADLS_CONTAINER_NAME = os.getenv("ADLS_CONTAINER_NAME", "lakehouse")
    ADLS_ACCESS_KEY = os.getenv("ADLS_ACCESS_KEY", "")
    
    # Databricks Configuration
    DATABRICKS_WORKSPACE = os.getenv("DATABRICKS_WORKSPACE", "")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "")
    
    # Event Hubs Configuration
    EVENT_HUB_NAMESPACE = os.getenv("EVENT_HUB_NAMESPACE", "healthcare-events")
    EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "claim-status-updates")
    EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING", "")
    
    # Synapse Configuration
    SYNAPSE_WORKSPACE = os.getenv("SYNAPSE_WORKSPACE", "")
    SYNAPSE_SQL_ENDPOINT = os.getenv("SYNAPSE_SQL_ENDPOINT", "")
    SYNAPSE_DATABASE = os.getenv("SYNAPSE_DATABASE", "analytics")
    SYNAPSE_USER = os.getenv("SYNAPSE_USER", "")
    SYNAPSE_PASSWORD = os.getenv("SYNAPSE_PASSWORD", "")
    
    # Data Lake Paths
    RAW_PATH = "/raw"
    BRONZE_PATH = "/bronze"
    SILVER_PATH = "/silver"
    GOLD_PATH = "/gold"
    AUDIT_PATH = "/audit"
    
    # Source Data Paths
    CLAIMS_RAW_PATH = f"{RAW_PATH}/claims"
    MEMBERS_RAW_PATH = f"{RAW_PATH}/members"
    PROVIDERS_RAW_PATH = f"{RAW_PATH}/providers"
    DIAGNOSIS_REF_PATH = f"{RAW_PATH}/diagnosis_ref"
    PROCEDURE_REF_PATH = f"{RAW_PATH}/procedure_ref"
    PAYER_REF_PATH = f"{RAW_PATH}/payer_ref"
    CLAIM_STATUS_EVENTS_PATH = f"{RAW_PATH}/claim_status_events"
    
    # Bronze Table Paths
    BRONZE_CLAIMS_PATH = f"{BRONZE_PATH}/claims"
    BRONZE_MEMBERS_PATH = f"{BRONZE_PATH}/members"
    BRONZE_PROVIDERS_PATH = f"{BRONZE_PATH}/providers"
    BRONZE_DIAGNOSIS_REF_PATH = f"{BRONZE_PATH}/diagnosis_ref"
    BRONZE_PROCEDURE_REF_PATH = f"{BRONZE_PATH}/procedure_ref"
    BRONZE_PAYER_REF_PATH = f"{BRONZE_PATH}/payer_ref"
    BRONZE_CLAIM_STATUS_EVENTS_PATH = f"{BRONZE_PATH}/claim_status_events"
    
    # Silver Table Paths
    SILVER_CLAIMS_CURRENT_PATH = f"{SILVER_PATH}/claims_current"
    SILVER_CLAIMS_HISTORY_PATH = f"{SILVER_PATH}/claims_history"
    SILVER_MEMBERS_PATH = f"{SILVER_PATH}/members"
    SILVER_PROVIDERS_PATH = f"{SILVER_PATH}/providers"
    SILVER_DIAGNOSIS_REF_PATH = f"{SILVER_PATH}/diagnosis_ref"
    SILVER_PROCEDURE_REF_PATH = f"{SILVER_PATH}/procedure_ref"
    SILVER_PAYER_REF_PATH = f"{SILVER_PATH}/payer_ref"
    SILVER_QUARANTINE_PATH = f"{SILVER_PATH}/quarantine"
    SILVER_DATA_QUALITY_PATH = f"{SILVER_PATH}/data_quality_results"
    
    # Gold Table Paths
    GOLD_FACT_CLAIMS_PATH = f"{GOLD_PATH}/fact_claims"
    GOLD_DIM_MEMBER_PATH = f"{GOLD_PATH}/dim_member"
    GOLD_DIM_PROVIDER_PATH = f"{GOLD_PATH}/dim_provider"
    GOLD_DIM_DIAGNOSIS_PATH = f"{GOLD_PATH}/dim_diagnosis"
    GOLD_DIM_PROCEDURE_PATH = f"{GOLD_PATH}/dim_procedure"
    GOLD_DIM_PAYER_PATH = f"{GOLD_PATH}/dim_payer"
    GOLD_DIM_DATE_PATH = f"{GOLD_PATH}/dim_date"
    GOLD_MART_PROVIDER_PERFORMANCE_PATH = f"{GOLD_PATH}/mart_provider_performance"
    GOLD_MART_DENIAL_TRENDS_PATH = f"{GOLD_PATH}/mart_denial_trends"
    GOLD_MART_MEMBER_UTILIZATION_PATH = f"{GOLD_PATH}/mart_member_utilization"
    GOLD_MART_CLAIM_ANOMALIES_PATH = f"{GOLD_PATH}/mart_claim_anomalies"
    
    # Audit Paths
    PIPELINE_RUNS_PATH = f"{AUDIT_PATH}/pipeline_runs"
    ROW_COUNTS_PATH = f"{AUDIT_PATH}/row_counts"
    ERROR_LOGS_PATH = f"{AUDIT_PATH}/error_logs"
    
    # Data Quality Configuration
    QUALITY_RULES = {
        "claims": {
            "required_fields": ["claim_id", "claim_line_id", "member_id", "provider_id", "claim_amount"],
            "amount_min": 0,
            "amount_max": 100000,
            "valid_statuses": ["PENDING", "PAID", "DENIED", "ADJUSTED"],
            "valid_places_of_service": ["11", "12", "21", "22", "23", "24", "25", "26", "31", "32", "33", "34", "41", "42", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99"]
        },
        "members": {
            "required_fields": ["member_id", "member_first_name", "member_last_name", "dob", "gender"],
            "valid_genders": ["M", "F"],
            "min_age": 0,
            "max_age": 120
        },
        "providers": {
            "required_fields": ["provider_id", "provider_name", "specialty", "network_status"],
            "valid_network_statuses": ["IN_NETWORK", "OUT_OF_NETWORK"]
        }
    }
    
    # Anomaly Detection Configuration
    ANOMALY_THRESHOLDS = {
        "duplicate_claim_threshold_hours": 24,
        "claim_amount_outlier_multiplier": 3.0,
        "high_denial_rate_threshold": 0.3,
        "unusual_procedure_frequency_days": 7
    }
    
    # Streaming Configuration
    STREAMING_CHECKPOINT_LOCATION = "/checkpoints/claim_status_stream"
    STREAMING_WATERMARK_DELAY = "2 minutes"
    STREAMING_OUTPUT_MODE = "append"
    
    @classmethod
    def get_adls_url(cls) -> str:
        """Get the full ADLS URL."""
        return f"abfss://{cls.ADLS_CONTAINER_NAME}@{cls.ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
    
    @classmethod
    def get_full_path(cls, base_path: str) -> str:
        """Get the full ADLS path for a given base path."""
        return f"{cls.get_adls_url()}{base_path}"
