"""
Data Utilities
==============
Helper functions for data cleaning and Arrow-compatible dataframe processing.
Updated to include ALL columns for filtering - no restrictions.
"""
import pandas as pd
import numpy as np
import streamlit as st
from typing import Optional


def clean_dataframe_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean dataframe to make it Arrow-compatible for Streamlit.
    
    Args:
        df: Input dataframe
        
    Returns:
        Cleaned dataframe compatible with Arrow serialization
    """
    if df is None or df.empty:
        return df
    
    # Create a copy to avoid modifying original
    cleaned_df = df.copy()
    
    # Handle object columns
    for col in cleaned_df.columns:
        if cleaned_df[col].dtype == 'object':
            # Try to convert to string, handling None/NaN values
            try:
                cleaned_df[col] = cleaned_df[col].astype(str)
                # Replace 'nan' and 'None' strings with actual NaN
                cleaned_df[col] = cleaned_df[col].replace(['nan', 'None', '<NA>'], pd.NA)
            except Exception:
                # If conversion fails, convert to string and handle nulls
                cleaned_df[col] = cleaned_df[col].fillna('').astype(str)
    
    # Handle mixed numeric columns
    for col in cleaned_df.columns:
        if cleaned_df[col].dtype == 'object':
            # Try to convert to numeric if possible
            try:
                # Check if column contains mostly numeric values
                numeric_converted = pd.to_numeric(cleaned_df[col], errors='coerce')
                non_null_ratio = numeric_converted.notna().sum() / len(cleaned_df[col])
                
                # If more than 80% can be converted to numeric, treat as numeric
                if non_null_ratio > 0.8:
                    cleaned_df[col] = numeric_converted
                else:
                    # Keep as string but ensure clean strings
                    cleaned_df[col] = cleaned_df[col].astype(str).replace('nan', '')
            except Exception:
                # Fallback: ensure it's a clean string column
                cleaned_df[col] = cleaned_df[col].astype(str).replace('nan', '')
    
    # Handle any remaining object columns that might cause issues
    for col in cleaned_df.columns:
        if cleaned_df[col].dtype == 'object':
            # Force conversion to string and handle special values
            cleaned_df[col] = (
                cleaned_df[col]
                .astype(str)
                .replace({'nan': '', 'None': '', '<NA>': '', 'null': ''})
            )
    
    return cleaned_df


def validate_dataframe_columns(df: pd.DataFrame) -> dict:
    """
    Validate dataframe columns and return information about data types.
    
    Args:
        df: Input dataframe
        
    Returns:
        Dictionary with column validation information
    """
    if df is None or df.empty:
        return {"valid": False, "message": "DataFrame is empty or None"}
    
    column_info = {}
    issues = []
    
    for col in df.columns:
        col_info = {
            "dtype": str(df[col].dtype),
            "null_count": df[col].isnull().sum(),
            "unique_count": df[col].nunique(),
            "sample_values": df[col].dropna().head(3).tolist()
        }
        
        # Check for problematic data types
        if df[col].dtype == 'object':
            # Check if it contains mixed types
            sample_types = set(type(x).__name__ for x in df[col].dropna().head(10))
            if len(sample_types) > 1:
                issues.append(f"Column '{col}' contains mixed data types: {sample_types}")
                col_info["mixed_types"] = True
        
        column_info[col] = col_info
    
    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "columns": column_info
    }


def safe_dataframe_display(df: pd.DataFrame, max_rows: int = 5) -> pd.DataFrame:
    """
    Safely prepare dataframe for display in Streamlit.
    
    Args:
        df: Input dataframe
        max_rows: Maximum rows to return
        
    Returns:
        Cleaned dataframe safe for Streamlit display
    """
    if df is None or df.empty:
        return df
    
    # Clean for Arrow compatibility
    display_df = clean_dataframe_for_arrow(df.head(max_rows))
    
    # Additional safety measures for display
    for col in display_df.columns:
        # Truncate very long strings that might cause display issues
        if display_df[col].dtype == 'object':
            display_df[col] = display_df[col].astype(str).str[:100]
    
    return display_df


def get_filterable_columns_safe(df: pd.DataFrame) -> list:
    """
    Get ALL columns suitable for filtering - no restrictions on unique values.
    
    Args:
        df: Input dataframe
        
    Returns:
        List of ALL column names suitable for filtering
    """
    if df is None or df.empty:
        return []
    
    filterable_cols = []
    
    for col in df.columns:
        try:
            # Include ALL columns except completely empty ones
            unique_count = df[col].nunique()
            
            if unique_count > 0:  # Has at least one unique value
                # Additional check: ensure the column doesn't have problematic data types
                if df[col].dtype == 'object':
                    # Make sure we can convert to string safely
                    try:
                        test_values = df[col].dropna().head(5).astype(str)
                        if len(test_values) > 0:  # Has some non-null values
                            filterable_cols.append(col)
                    except Exception:
                        continue  # Skip this column if conversion fails
                else:
                    # Include all numeric columns too
                    filterable_cols.append(col)
                    
        except Exception as e:
            # Skip columns that cause any issues
            continue
    
    return filterable_cols


def safe_unique_values(df: pd.DataFrame, column: str, max_values: int = 10000) -> list:
    """
    Safely get unique values from a column - increased limit for large datasets.
    
    Args:
        df: Input dataframe
        column: Column name
        max_values: Maximum number of unique values to return (increased to 10000)
        
    Returns:
        List of unique values, safely converted to strings
    """
    if df is None or df.empty or column not in df.columns:
        return []
    
    try:
        # Get unique values, excluding NaN
        unique_vals = df[column].dropna().unique()
        
        # Allow up to 10000 values for large datasets
        if len(unique_vals) > max_values:
            unique_vals = unique_vals[:max_values]
        
        # Convert to strings safely
        str_vals = []
        for val in unique_vals:
            try:
                str_val = str(val)
                if str_val not in ['nan', 'None', '', 'null']:
                    str_vals.append(str_val)
            except Exception:
                continue
        
        return sorted(str_vals)
        
    except Exception as e:
        st.warning(f"Error getting unique values for column '{column}': {str(e)}")
        return []


@st.cache_data
def cached_clean_dataframe(df_dict: dict) -> pd.DataFrame:
    """
    Cached version of dataframe cleaning.
    
    Args:
        df_dict: Dictionary representation of dataframe
        
    Returns:
        Cleaned dataframe
    """
    df = pd.DataFrame(df_dict)
    return clean_dataframe_for_arrow(df)


def get_dataframe_info(df: pd.DataFrame) -> dict:
    """
    Get comprehensive information about a dataframe.
    
    Args:
        df: Input dataframe
        
    Returns:
        Dictionary with dataframe statistics
    """
    if df is None or df.empty:
        return {"rows": 0, "columns": 0, "memory_usage": 0}
    
    memory_usage = df.memory_usage(deep=True).sum()
    
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "memory_usage_mb": round(memory_usage / 1024 / 1024, 2),
        "dtypes": df.dtypes.value_counts().to_dict(),
        "null_counts": df.isnull().sum().to_dict(),
        "duplicate_rows": df.duplicated().sum()
    }
