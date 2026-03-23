"""
Data quality rules and validation functions for the Healthcare Claims Lakehouse Platform.
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List, Tuple, Any
import re
from datetime import datetime, timedelta

class DataQualityRules:
    """Data quality rules and validation functions."""
    
    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config
    
    def validate_claims(self, df):
        """
        Validate claims data against quality rules.
        
        Args:
            df: Claims DataFrame
            
        Returns:
            Tuple of (clean_df, quarantine_df, quality_results)
        """
        quality_results = {}
        
        # Create a copy for validation
        validation_df = df.alias("validation")
        
        # Rule 1: Required fields not null
        required_fields = self.config.QUALITY_RULES["claims"]["required_fields"]
        null_checks = []
        for field in required_fields:
            null_count = validation_df.filter(col(field).isNull()).count()
            quality_results[f"null_{field}"] = null_count
            null_checks.append(col(field).isNotNull())
        
        # Rule 2: Valid claim amounts
        amount_min = self.config.QUALITY_RULES["claims"]["amount_min"]
        amount_max = self.config.QUALITY_RULES["claims"]["amount_max"]
        invalid_amounts = validation_df.filter(
            (col("claim_amount") < amount_min) | (col("claim_amount") > amount_max)
        )
        quality_results["invalid_amounts"] = invalid_amounts.count()
        
        # Rule 3: Valid claim statuses
        valid_statuses = self.config.QUALITY_RULES["claims"]["valid_statuses"]
        invalid_statuses = validation_df.filter(
            ~col("claim_status").isin(valid_statuses)
        )
        quality_results["invalid_statuses"] = invalid_statuses.count()
        
        # Rule 4: Valid places of service
        valid_pos = self.config.QUALITY_RULES["claims"]["valid_places_of_service"]
        invalid_pos = validation_df.filter(
            ~col("place_of_service").isin(valid_pos)
        )
        quality_results["invalid_place_of_service"] = invalid_pos.count()
        
        # Rule 5: Service date not in future
        future_service_dates = validation_df.filter(col("service_date") > current_date())
        quality_results["future_service_dates"] = future_service_dates.count()
        
        # Rule 6: Service date before submission date
        invalid_date_order = validation_df.filter(
            col("service_date") > col("submission_date")
        )
        quality_results["invalid_date_order"] = invalid_date_order.count()
        
        # Rule 7: Valid diagnosis codes (ICD-10 format)
        diagnosis_pattern = r"^[A-Z]\d{2}(\.\d{1,3})?$"
        invalid_diagnosis = validation_df.filter(
            col("diagnosis_code").isNotNull() &
            ~regexp_replace(col("diagnosis_code"), diagnosis_pattern, "").equalTo("")
        )
        quality_results["invalid_diagnosis_codes"] = invalid_diagnosis.count()
        
        # Rule 8: Valid procedure codes (CPT format - 5 digits)
        procedure_pattern = r"^\d{5}$"
        invalid_procedure = validation_df.filter(
            col("procedure_code").isNotNull() &
            ~regexp_replace(col("procedure_code"), procedure_pattern, "").equalTo("")
        )
        quality_results["invalid_procedure_codes"] = invalid_procedure.count()
        
        # Build quarantine conditions
        quarantine_conditions = [
            # Null required fields
            reduce(lambda a, b: a | b, null_checks) if len(null_checks) > 1 else null_checks[0],
            # Invalid amounts
            (col("claim_amount") < amount_min) | (col("claim_amount") > amount_max),
            # Invalid statuses
            ~col("claim_status").isin(valid_statuses),
            # Invalid places of service
            ~col("place_of_service").isin(valid_pos),
            # Future service dates
            col("service_date") > current_date(),
            # Invalid date order
            col("service_date") > col("submission_date"),
            # Invalid codes
            (col("diagnosis_code").isNotNull() & 
             ~regexp_replace(col("diagnosis_code"), diagnosis_pattern, "").equalTo("")),
            (col("procedure_code").isNotNull() & 
             ~regexp_replace(col("procedure_code"), procedure_pattern, "").equalTo(""))
        ]
        
        # Separate clean and quarantine records
        quarantine_records = validation_df.filter(reduce(lambda a, b: a | b, quarantine_conditions))
        clean_records = validation_df.filter(~(reduce(lambda a, b: a | b, quarantine_conditions)))
        
        # Add quarantine reasons
        quarantine_records = quarantine_records.withColumn(
            "quarantine_reasons",
            concat_ws(", ", 
                when(col("claim_amount") < amount_min, "Amount below minimum"),
                when(col("claim_amount") > amount_max, "Amount above maximum"),
                when(~col("claim_status").isin(valid_statuses), "Invalid status"),
                when(~col("place_of_service").isin(valid_pos), "Invalid place of service"),
                when(col("service_date") > current_date(), "Future service date"),
                when(col("service_date") > col("submission_date"), "Invalid date order"),
                when(col("diagnosis_code").isNotNull() & 
                     ~regexp_replace(col("diagnosis_code"), diagnosis_pattern, "").equalTo(""), "Invalid diagnosis code"),
                when(col("procedure_code").isNotNull() & 
                     ~regexp_replace(col("procedure_code"), procedure_pattern, "").equalTo(""), "Invalid procedure code")
            )
        )
        
        return clean_records, quarantine_records, quality_results
    
    def validate_members(self, df):
        """
        Validate members data against quality rules.
        
        Args:
            df: Members DataFrame
            
        Returns:
            Tuple of (clean_df, quarantine_df, quality_results)
        """
        quality_results = {}
        
        # Rule 1: Required fields not null
        required_fields = self.config.QUALITY_RULES["members"]["required_fields"]
        null_checks = []
        for field in required_fields:
            null_count = df.filter(col(field).isNull()).count()
            quality_results[f"null_{field}"] = null_count
            null_checks.append(col(field).isNotNull())
        
        # Rule 2: Valid gender values
        valid_genders = self.config.QUALITY_RULES["members"]["valid_genders"]
        invalid_genders = df.filter(~col("gender").isin(valid_genders))
        quality_results["invalid_genders"] = invalid_genders.count()
        
        # Rule 3: Reasonable age range
        min_age = self.config.QUALITY_RULES["members"]["min_age"]
        max_age = self.config.QUALITY_RULES["members"]["max_age"]
        invalid_age = df.filter(
            (floor(months_between(current_date(), col("dob")) / 12) < min_age) |
            (floor(months_between(current_date(), col("dob")) / 12) > max_age)
        )
        quality_results["invalid_age"] = invalid_age.count()
        
        # Rule 4: Future dates of birth
        future_dob = df.filter(col("dob") > current_date())
        quality_results["future_dob"] = future_dob.count()
        
        # Rule 5: Eligibility date logic
        invalid_eligibility = df.filter(
            col("eligibility_start_date").isNotNull() &
            col("eligibility_end_date").isNotNull() &
            (col("eligibility_start_date") > col("eligibility_end_date"))
        )
        quality_results["invalid_eligibility_dates"] = invalid_eligibility.count()
        
        # Build quarantine conditions
        quarantine_conditions = [
            reduce(lambda a, b: a | b, null_checks) if len(null_checks) > 1 else null_checks[0],
            ~col("gender").isin(valid_genders),
            (floor(months_between(current_date(), col("dob")) / 12) < min_age) |
            (floor(months_between(current_date(), col("dob")) / 12) > max_age),
            col("dob") > current_date(),
            (col("eligibility_start_date").isNotNull() &
             col("eligibility_end_date").isNotNull() &
             (col("eligibility_start_date") > col("eligibility_end_date")))
        ]
        
        # Separate clean and quarantine records
        quarantine_records = df.filter(reduce(lambda a, b: a | b, quarantine_conditions))
        clean_records = df.filter(~(reduce(lambda a, b: a | b, quarantine_conditions)))
        
        # Add quarantine reasons
        quarantine_records = quarantine_records.withColumn(
            "quarantine_reasons",
            concat_ws(", ", 
                when(~col("gender").isin(valid_genders), "Invalid gender"),
                when((floor(months_between(current_date(), col("dob")) / 12) < min_age) |
                     (floor(months_between(current_date(), col("dob")) / 12) > max_age), "Invalid age"),
                when(col("dob") > current_date(), "Future date of birth"),
                when(col("eligibility_start_date").isNotNull() &
                     col("eligibility_end_date").isNotNull() &
                     (col("eligibility_start_date") > col("eligibility_end_date")), "Invalid eligibility dates")
            )
        )
        
        return clean_records, quarantine_records, quality_results
    
    def validate_providers(self, df):
        """
        Validate providers data against quality rules.
        
        Args:
            df: Providers DataFrame
            
        Returns:
            Tuple of (clean_df, quarantine_df, quality_results)
        """
        quality_results = {}
        
        # Rule 1: Required fields not null
        required_fields = self.config.QUALITY_RULES["providers"]["required_fields"]
        null_checks = []
        for field in required_fields:
            null_count = df.filter(col(field).isNull()).count()
            quality_results[f"null_{field}"] = null_count
            null_checks.append(col(field).isNotNull())
        
        # Rule 2: Valid network status
        valid_network_statuses = self.config.QUALITY_RULES["providers"]["valid_network_statuses"]
        invalid_network_status = df.filter(~col("network_status").isin(valid_network_statuses))
        quality_results["invalid_network_status"] = invalid_network_status.count()
        
        # Rule 3: Valid date ranges
        invalid_dates = df.filter(
            col("termination_date").isNotNull() &
            col("effective_date").isNotNull() &
            (col("termination_date") < col("effective_date"))
        )
        quality_results["invalid_dates"] = invalid_dates.count()
        
        # Rule 4: Valid ZIP codes (5 digits)
        zip_pattern = r"^\d{5}$"
        invalid_zip = df.filter(
            col("zip_code").isNotNull() &
            ~regexp_replace(col("zip_code"), zip_pattern, "").equalTo("")
        )
        quality_results["invalid_zip_codes"] = invalid_zip.count()
        
        # Build quarantine conditions
        quarantine_conditions = [
            reduce(lambda a, b: a | b, null_checks) if len(null_checks) > 1 else null_checks[0],
            ~col("network_status").isin(valid_network_statuses),
            (col("termination_date").isNotNull() &
             col("effective_date").isNotNull() &
             (col("termination_date") < col("effective_date"))),
            (col("zip_code").isNotNull() &
             ~regexp_replace(col("zip_code"), zip_pattern, "").equalTo(""))
        ]
        
        # Separate clean and quarantine records
        quarantine_records = df.filter(reduce(lambda a, b: a | b, quarantine_conditions))
        clean_records = df.filter(~(reduce(lambda a, b: a | b, quarantine_conditions)))
        
        # Add quarantine reasons
        quarantine_records = quarantine_records.withColumn(
            "quarantine_reasons",
            concat_ws(", ", 
                when(~col("network_status").isin(valid_network_statuses), "Invalid network status"),
                when(col("termination_date").isNotNull() &
                     col("effective_date").isNotNull() &
                     (col("termination_date") < col("effective_date")), "Invalid date range"),
                when(col("zip_code").isNotNull() &
                     ~regexp_replace(col("zip_code"), zip_pattern, "").equalTo(""), "Invalid ZIP code")
            )
        )
        
        return clean_records, quarantine_records, quality_results
    
    def detect_anomalies(self, claims_df, providers_df, members_df):
        """
        Detect anomalies in claims data.
        
        Args:
            claims_df: Claims DataFrame
            providers_df: Providers DataFrame
            members_df: Members DataFrame
            
        Returns:
            DataFrame with anomaly flags and reasons
        """
        # Join claims with provider and member data
        enriched_claims = claims_df.alias("claims") \
            .join(providers_df.alias("providers"), 
                  col("claims.provider_id") == col("providers.provider_id"), "left") \
            .join(members_df.alias("members"), 
                  col("claims.member_id") == col("members.member_id"), "left")
        
        # Anomaly 1: Duplicate claims (same provider, member, diagnosis, procedure within 24 hours)
        duplicate_window = self.config.ANOMALY_THRESHOLDS["duplicate_claim_threshold_hours"]
        duplicate_claims = enriched_claims.withColumn(
            "duplicate_flag",
            count("*").over(
                Window.partitionBy(
                    "provider_id", "member_id", "diagnosis_code", "procedure_code"
                ).orderBy("service_date").rangeBetween(0, duplicate_window * 3600)
            ) > 1
        )
        
        # Anomaly 2: Claim amount outliers (3x provider average)
        provider_avg = enriched_claims.groupBy("provider_id") \
            .agg(avg("claim_amount").alias("provider_avg_amount"))
        
        claims_with_avg = duplicate_claims.join(provider_avg, "provider_id", "left")
        outlier_multiplier = self.config.ANOMALY_THRESHOLDS["claim_amount_outlier_multiplier"]
        
        claims_with_outliers = claims_with_avg.withColumn(
            "amount_outlier_flag",
            col("claim_amount") > (col("provider_avg_amount") * outlier_multiplier)
        )
        
        # Anomaly 3: High denial rate providers
        provider_denial_rate = enriched_claims.groupBy("provider_id") \
            .agg(
                count("*").alias("total_claims"),
                sum(when(col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied_claims")
            ) \
            .withColumn("denial_rate", col("denied_claims") / col("total_claims"))
        
        high_denial_threshold = self.config.ANOMALY_THRESHOLDS["high_denial_rate_threshold"]
        high_denial_providers = provider_denial_rate.filter(
            col("denial_rate") > high_denial_threshold
        ).select("provider_id")
        
        claims_with_denial_flag = claims_with_outliers.join(
            high_denial_providers, "provider_id", "left"
        ).withColumn(
            "high_denial_provider_flag",
            col("provider_id").isNotNull()
        )
        
        # Anomaly 4: Frequent procedures for same member
        procedure_frequency = self.config.ANOMALY_THRESHOLDS["unusual_procedure_frequency_days"]
        frequent_procedures = claims_with_denial_flag.withColumn(
            "frequent_procedure_flag",
            count("*").over(
                Window.partitionBy("member_id", "procedure_code")
                .orderBy("service_date").rangeBetween(0, procedure_frequency * 86400)
            ) > 3
        )
        
        # Add anomaly reasons
        anomalies_df = frequent_procedures.withColumn(
            "anomaly_reasons",
            concat_ws(", ", 
                when(col("duplicate_flag"), "Potential duplicate claim"),
                when(col("amount_outlier_flag"), "Unusually high claim amount"),
                when(col("high_denial_provider_flag"), "High denial rate provider"),
                when(col("frequent_procedure_flag"), "Frequent procedure for member")
            )
        ).withColumn(
            "has_anomaly",
            col("duplicate_flag") | col("amount_outlier_flag") | 
            col("high_denial_provider_flag") | col("frequent_procedure_flag")
        )
        
        return anomalies_df
