"""
Validation Utilities
===================
Data validation helpers for the web scraping workflow.
"""
import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Optional, Tuple
import re
from datetime import datetime


def validate_csv_structure(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate the basic structure of an uploaded CSV."""
    validation_result = {
        "is_valid": True,
        "errors": [],
        "warnings": [],
        "info": {},
        "recommendations": []
    }
    
    try:
        # Basic structure checks
        if df.empty:
            validation_result["errors"].append("CSV file is empty")
            validation_result["is_valid"] = False
            return validation_result
        
        if len(df.columns) == 0:
            validation_result["errors"].append("CSV file has no columns")
            validation_result["is_valid"] = False
            return validation_result
        
        # Record basic info
        validation_result["info"].update({
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "dtypes": df.dtypes.to_dict()
        })
        
        # Check for required columns for web scraping
        required_columns = ['Consignee Name']  # Essential for company research
        missing_required = [col for col in required_columns if col not in df.columns]
        
        if missing_required:
            validation_result["errors"].append(
                f"Missing required columns for web scraping: {missing_required}"
            )
            validation_result["is_valid"] = False
        
        # Check for recommended columns
        recommended_columns = ['Country', 'Product', 'Value', 'Date']
        missing_recommended = [col for col in recommended_columns if col not in df.columns]
        
        if missing_recommended:
            validation_result["recommendations"].append(
                f"Consider adding these columns for better analysis: {missing_recommended}"
            )
        
        # Check data quality
        null_percentages = (df.isnull().sum() / len(df) * 100).to_dict()
        high_null_columns = [col for col, pct in null_percentages.items() if pct > 50]
        
        if high_null_columns:
            validation_result["warnings"].append(
                f"Columns with >50% missing data: {high_null_columns}"
            )
        
        validation_result["info"]["null_percentages"] = null_percentages
        
        # Check for duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            validation_result["warnings"].append(f"Found {duplicate_count} duplicate rows")
            validation_result["info"]["duplicate_count"] = duplicate_count
        
        # Validate company names if present
        if 'Consignee Name' in df.columns:
            company_validation = validate_company_names(df['Consignee Name'])
            validation_result["info"]["company_validation"] = company_validation
            
            if not company_validation["is_valid"]:
                validation_result["warnings"].extend(company_validation["warnings"])
        
        return validation_result
        
    except Exception as e:
        validation_result["errors"].append(f"Validation error: {str(e)}")
        validation_result["is_valid"] = False
        return validation_result


def validate_company_names(company_series: pd.Series) -> Dict[str, Any]:
    """Validate company names for web research suitability."""
    validation_result = {
        "is_valid": True,
        "warnings": [],
        "info": {}
    }
    
    try:
        # Remove null values
        clean_names = company_series.dropna()
        
        if len(clean_names) == 0:
            validation_result["warnings"].append("No valid company names found")
            validation_result["is_valid"] = False
            return validation_result
        
        # Count statistics
        total_count = len(company_series)
        valid_count = len(clean_names)
        unique_count = clean_names.nunique()
        
        validation_result["info"].update({
            "total_companies": total_count,
            "valid_companies": valid_count,
            "unique_companies": unique_count,
            "null_percentage": round((total_count - valid_count) / total_count * 100, 2)
        })
        
        # Check for problematic company names
        problematic_names = []
        short_names = []
        
        for name in clean_names.unique():
            name_str = str(name).strip()
            
            # Check for very short names (may be codes rather than company names)
            if len(name_str) < 3:
                short_names.append(name_str)
            
            # Check for obviously invalid names
            if name_str.lower() in ['unknown', 'n/a', 'na', 'none', 'null', '']:
                problematic_names.append(name_str)
            
            # Check for numeric-only names (likely IDs, not company names)
            if name_str.isdigit():
                problematic_names.append(name_str)
        
        if short_names:
            validation_result["warnings"].append(
                f"Found {len(short_names)} very short company names (may be codes): {short_names[:5]}"
            )
        
        if problematic_names:
            validation_result["warnings"].append(
                f"Found {len(problematic_names)} invalid company names: {problematic_names[:5]}"
            )
        
        # Check for searchable company names
        searchable_count = 0
        for name in clean_names.unique():
            name_str = str(name).strip()
            if len(name_str) >= 3 and not name_str.isdigit():
                searchable_count += 1
        
        validation_result["info"]["searchable_companies"] = searchable_count
        
        if searchable_count < unique_count * 0.8:
            validation_result["warnings"].append(
                f"Only {searchable_count}/{unique_count} company names appear suitable for web research"
            )
        
        return validation_result
        
    except Exception as e:
        validation_result["warnings"].append(f"Company name validation error: {str(e)}")
        return validation_result


def validate_filter_criteria(df: pd.DataFrame, filter_column: str, 
                            filter_values: List[str]) -> Dict[str, Any]:
    """Validate filter criteria before applying."""
    validation_result = {
        "is_valid": True,
        "errors": [],
        "warnings": [],
        "info": {}
    }
    
    try:
        # Check if column exists
        if filter_column not in df.columns:
            validation_result["errors"].append(f"Filter column '{filter_column}' not found in data")
            validation_result["is_valid"] = False
            return validation_result
        
        # Check if filter values exist in column
        available_values = set(df[filter_column].dropna().astype(str).unique())
        invalid_values = [val for val in filter_values if val not in available_values]
        
        if invalid_values:
            validation_result["warnings"].append(
                f"Filter values not found in data: {invalid_values}"
            )
        
        # Estimate result count
        if filter_values:
            filtered_df = df[df[filter_column].astype(str).isin(filter_values)]
            result_count = len(filtered_df)
            
            validation_result["info"]["estimated_results"] = result_count
            validation_result["info"]["percentage_of_data"] = round(result_count / len(df) * 100, 2)
            
            if result_count == 0:
                validation_result["warnings"].append("Filter criteria will return no results")
            elif result_count == len(df):
                validation_result["warnings"].append("Filter criteria will return all data (no filtering effect)")
        
        return validation_result
        
    except Exception as e:
        validation_result["errors"].append(f"Filter validation error: {str(e)}")
        validation_result["is_valid"] = False
        return validation_result


def validate_research_readiness(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate if data is ready for web research stage."""
    validation_result = {
        "is_ready": True,
        "errors": [],
        "warnings": [],
        "info": {},
        "recommendations": []
    }
    
    try:
        # Check for required columns
        if 'Consignee Name' not in df.columns:
            validation_result["errors"].append("Missing 'Consignee Name' column required for research")
            validation_result["is_ready"] = False
            return validation_result
        
        # Validate company names
        company_validation = validate_company_names(df['Consignee Name'])
        validation_result["info"]["company_validation"] = company_validation
        
        searchable_count = company_validation["info"].get("searchable_companies", 0)
        
        if searchable_count == 0:
            validation_result["errors"].append("No searchable company names found")
            validation_result["is_ready"] = False
        elif searchable_count < 5:
            validation_result["warnings"].append(
                f"Only {searchable_count} companies suitable for research. Consider data quality."
            )
        
        # Check if already researched
        if 'web_research_status' in df.columns:
            already_researched = (df['web_research_status'] == 'completed').sum()
            validation_result["info"]["already_researched"] = already_researched
            
            if already_researched == len(df):
                validation_result["warnings"].append("All companies already researched")
        
        # Recommendations for better research
        if 'Country' in df.columns:
            validation_result["recommendations"].append(
                "Country information available - can improve research accuracy"
            )
        
        if 'Product' in df.columns:
            validation_result["recommendations"].append(
                "Product information available - can help identify relevant contacts"
            )
        
        return validation_result
        
    except Exception as e:
        validation_result["errors"].append(f"Research readiness validation error: {str(e)}")
        validation_result["is_ready"] = False
        return validation_result


def validate_email_readiness(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate if data is ready for email campaign stage."""
    validation_result = {
        "is_ready": True,
        "errors": [],
        "warnings": [],
        "info": {},
        "recommendations": []
    }
    
    try:
        # Check for required columns
        required_cols = ['Consignee Name', 'contact_details']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            validation_result["errors"].append(f"Missing required columns: {missing_cols}")
            validation_result["is_ready"] = False
            return validation_result
        
        # Check for contact details
        contacts_available = df['contact_details'].dropna()
        contacts_count = len(contacts_available)
        
        validation_result["info"]["total_companies"] = len(df)
        validation_result["info"]["companies_with_contacts"] = contacts_count
        validation_result["info"]["contact_percentage"] = round(contacts_count / len(df) * 100, 2)
        
        if contacts_count == 0:
            validation_result["errors"].append("No contact details found for email campaign")
            validation_result["is_ready"] = False
            return validation_result
        elif contacts_count < len(df) * 0.5:
            validation_result["warnings"].append(
                f"Only {contacts_count}/{len(df)} companies have contact details"
            )
        
        # Validate email addresses if present
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails_found = 0
        
        for contact in contacts_available:
            if re.search(email_pattern, str(contact)):
                emails_found += 1
        
        validation_result["info"]["emails_found"] = emails_found
        
        if emails_found == 0:
            validation_result["warnings"].append("No email addresses detected in contact details")
        elif emails_found < contacts_count * 0.5:
            validation_result["warnings"].append(
                f"Only {emails_found}/{contacts_count} contacts appear to have email addresses"
            )
        
        # Check if already emailed
        if 'email_sent_status' in df.columns:
            already_emailed = (df['email_sent_status'] == 'sent').sum()
            validation_result["info"]["already_emailed"] = already_emailed
            
            if already_emailed == contacts_count:
                validation_result["warnings"].append("All companies with contacts already emailed")
        
        return validation_result
        
    except Exception as e:
        validation_result["errors"].append(f"Email readiness validation error: {str(e)}")
        validation_result["is_ready"] = False
        return validation_result


def validate_stage_transition(current_stage: str, target_stage: str, 
                            df: pd.DataFrame) -> Dict[str, Any]:
    """Validate if transition between stages is possible."""
    validation_result = {
        "can_transition": True,
        "errors": [],
        "warnings": [],
        "info": {}
    }
    
    try:
        stage_order = ['upload', 'map', 'analyze']
        
        if target_stage not in stage_order:
            validation_result["errors"].append(f"Invalid target stage: {target_stage}")
            validation_result["can_transition"] = False
            return validation_result
        
        current_index = stage_order.index(current_stage) if current_stage in stage_order else -1
        target_index = stage_order.index(target_stage)
        
        # Check if skipping stages
        if target_index > current_index + 1:
            validation_result["warnings"].append(
                f"Skipping intermediate stages from {current_stage} to {target_stage}"
            )
        
        # Validate requirements for target stage
        if target_stage == 'map':
            research_validation = validate_research_readiness(df)
            validation_result["info"]["research_validation"] = research_validation
            
            if not research_validation["is_ready"]:
                validation_result["errors"].extend(research_validation["errors"])
                validation_result["can_transition"] = False
        
        elif target_stage == 'analyze':
            email_validation = validate_email_readiness(df)
            validation_result["info"]["email_validation"] = email_validation
            
            if not email_validation["is_ready"]:
                validation_result["errors"].extend(email_validation["errors"])
                validation_result["can_transition"] = False
        
        return validation_result
        
    except Exception as e:
        validation_result["errors"].append(f"Stage transition validation error: {str(e)}")
        validation_result["can_transition"] = False
        return validation_result


def get_data_quality_score(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate an overall data quality score."""
    try:
        quality_metrics = {
            "completeness": 0,
            "validity": 0, 
            "consistency": 0,
            "overall_score": 0
        }
        
        # Completeness score (based on null values)
        null_percentage = df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
        completeness_score = max(0, 100 - null_percentage)
        quality_metrics["completeness"] = round(completeness_score, 2)
        
        # Validity score (based on data types and formats)
        validity_issues = 0
        total_checks = 0
        
        # Check company names if present
        if 'Consignee Name' in df.columns:
            company_validation = validate_company_names(df['Consignee Name'])
            searchable_ratio = (
                company_validation["info"].get("searchable_companies", 0) /
                max(company_validation["info"].get("unique_companies", 1), 1)
            )
            validity_issues += (1 - searchable_ratio)
            total_checks += 1
        
        # Check for duplicate rows
        if len(df) > 0:
            duplicate_ratio = df.duplicated().sum() / len(df)
            validity_issues += duplicate_ratio
            total_checks += 1
        
        validity_score = max(0, (1 - validity_issues / max(total_checks, 1)) * 100)
        quality_metrics["validity"] = round(validity_score, 2)
        
        # Consistency score (basic check)
        consistency_score = 85  # Base score, can be enhanced with more checks
        quality_metrics["consistency"] = consistency_score
        
        # Overall score (weighted average)
        overall_score = (
            quality_metrics["completeness"] * 0.4 +
            quality_metrics["validity"] * 0.4 +
            quality_metrics["consistency"] * 0.2
        )
        quality_metrics["overall_score"] = round(overall_score, 2)
        
        # Add interpretation
        if overall_score >= 80:
            quality_metrics["interpretation"] = "Excellent"
        elif overall_score >= 60:
            quality_metrics["interpretation"] = "Good" 
        elif overall_score >= 40:
            quality_metrics["interpretation"] = "Fair"
        else:
            quality_metrics["interpretation"] = "Poor"
        
        return quality_metrics
        
    except Exception as e:
        return {
            "completeness": 0,
            "validity": 0,
            "consistency": 0,
            "overall_score": 0,
            "interpretation": "Error",
            "error": str(e)
        }
