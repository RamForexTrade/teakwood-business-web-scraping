"""
CSV Processing Engine
====================
Advanced CSV operations with session management, dynamic filtering, and stage-specific processing
for the web scraping workflow.
"""
import pandas as pd
import streamlit as st
from typing import Dict, List, Any, Optional, Tuple, Union
import numpy as np
from datetime import datetime
import json
import os
import re
from io import BytesIO, StringIO

from utils.data_utils import clean_dataframe_for_arrow, safe_unique_values, get_filterable_columns_safe
from utils.validation import validate_csv_structure, validate_filter_criteria, get_data_quality_score
from services.session_manager import session_manager
from state_management import get_state, add_data_checkpoint, save_session_metadata


class CSVProcessor:
    """Advanced CSV processing with session management and workflow-specific operations."""
    
    def __init__(self):
        self.supported_encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-16']
        self.chunk_size = 10000  # For large file processing
        
    def load_with_encoding_detection(self, file_input: Union[str, bytes, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Load CSV with automatic encoding detection and comprehensive error handling.
        
        Args:
            file_input: File path, bytes, or file-like object
            
        Returns:
            Tuple of (DataFrame, metadata dict)
        """
        metadata = {
            "success": False,
            "encoding_used": None,
            "file_size": 0,
            "load_time": 0,
            "issues": [],
            "warnings": []
        }
        
        start_time = datetime.now()
        
        try:
            df = None
            
            # Handle different input types
            if isinstance(file_input, str):
                # File path
                metadata["file_size"] = os.path.getsize(file_input)
                file_data = open(file_input, 'rb').read()
            elif hasattr(file_input, 'getvalue'):
                # Streamlit uploaded file
                file_data = file_input.getvalue()
                metadata["file_size"] = len(file_data)
            elif isinstance(file_input, bytes):
                # Raw bytes
                file_data = file_input
                metadata["file_size"] = len(file_data)
            else:
                raise ValueError("Unsupported file input type")
            
            # Try different encodings
            for encoding in self.supported_encodings:
                try:
                    file_io = BytesIO(file_data)
                    df = pd.read_csv(file_io, encoding=encoding, 
                                   low_memory=False, na_values=['', 'NULL', 'null', 'N/A', 'n/a'])
                    metadata["encoding_used"] = encoding
                    break
                except UnicodeDecodeError:
                    continue
                except pd.errors.ParserError as e:
                    metadata["issues"].append(f"Parser error with {encoding}: {str(e)}")
                    continue
                except Exception as e:
                    if encoding == self.supported_encodings[-1]:  # Last encoding
                        raise e
                    continue
            
            if df is None:
                raise ValueError("Could not decode file with any supported encoding")
            
            # Basic validation and cleaning
            if df.empty:
                metadata["warnings"].append("CSV file is empty")
                return pd.DataFrame(), metadata
            
            if len(df.columns) == 0:
                metadata["issues"].append("CSV file has no columns")
                return pd.DataFrame(), metadata
            
            # Clean column names
            original_columns = df.columns.tolist()
            df.columns = df.columns.astype(str).str.strip()
            
            # Remove completely empty rows
            initial_rows = len(df)
            df = df.dropna(how='all')
            removed_rows = initial_rows - len(df)
            
            if removed_rows > 0:
                metadata["warnings"].append(f"Removed {removed_rows} completely empty rows")
            
            # Clean for Arrow compatibility
            df = clean_dataframe_for_arrow(df)
            
            # Calculate load time
            metadata["load_time"] = (datetime.now() - start_time).total_seconds()
            metadata["success"] = True
            metadata["rows_loaded"] = len(df)
            metadata["columns_loaded"] = len(df.columns)
            metadata["original_columns"] = original_columns
            
            return df, metadata
            
        except Exception as e:
            metadata["issues"].append(f"Error loading CSV: {str(e)}")
            metadata["load_time"] = (datetime.now() - start_time).total_seconds()
            return pd.DataFrame(), metadata
    
    def add_tracking_columns(self, df: pd.DataFrame, session_id: str, stage: str = "upload") -> pd.DataFrame:
        """
        Add comprehensive tracking columns for the web scraping workflow.
        
        Args:
            df: Source dataframe
            session_id: Current session ID
            stage: Current workflow stage
            
        Returns:
            DataFrame with tracking columns added
        """
        try:
            tracked_df = df.copy()
            
            # Core tracking columns
            tracking_columns = {
                'session_id': session_id,
                'upload_timestamp': datetime.now().isoformat(),
                'current_stage': stage,
                'last_updated': datetime.now().isoformat(),
                
                # Filter tracking
                'filter_applied': False,
                'filter_timestamp': '',
                'filter_criteria': '',
                
                # Research tracking
                'web_research_status': 'pending',
                'research_timestamp': '',
                'research_attempts': 0,
                'contact_details': '',
                'contact_emails': '',
                'contact_phones': '',
                'company_website': '',
                'company_industry': '',
                'research_quality_score': 0,
                
                # Email campaign tracking
                'email_sent_status': 'not_sent',
                'email_timestamp': '',
                'email_campaign_id': '',
                'email_delivery_status': '',
                'email_open_status': '',
                'email_click_status': '',
                'email_response_status': '',
                
                # Data quality tracking
                'data_quality_score': 0,
                'validation_status': 'pending',
                'issues_found': '',
                'manual_review_flag': False
            }
            
            # Add columns if they don't exist
            for col_name, default_value in tracking_columns.items():
                if col_name not in tracked_df.columns:
                    tracked_df[col_name] = default_value
            
            return tracked_df
            
        except Exception as e:
            st.error(f"Error adding tracking columns: {str(e)}")
            return df  # Return original if tracking fails
    
    def apply_dynamic_filters(self, df: pd.DataFrame, filter_config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Apply dynamic, multi-level filters with comprehensive logging.
        
        Args:
            df: Source dataframe
            filter_config: Filter configuration dictionary
            
        Returns:
            Tuple of (filtered DataFrame, filter results metadata)
        """
        filter_results = {
            "success": True,
            "original_rows": len(df),
            "filtered_rows": 0,
            "filters_applied": [],
            "warnings": [],
            "filter_summary": {}
        }
        
        try:
            filtered_df = df.copy()
            
            # Process each filter
            for filter_name, filter_spec in filter_config.items():
                if not filter_spec.get('enabled', True):
                    continue
                
                column = filter_spec.get('column')
                operation = filter_spec.get('operation', 'in')
                values = filter_spec.get('values', [])
                
                if not column or not values:
                    continue
                
                if column not in filtered_df.columns:
                    filter_results["warnings"].append(f"Column '{column}' not found for filter '{filter_name}'")
                    continue
                
                # Apply filter based on operation type
                rows_before = len(filtered_df)
                
                if operation == 'in':
                    # Include rows where column value is in the specified values
                    filtered_df = filtered_df[filtered_df[column].astype(str).isin([str(v) for v in values])]
                
                elif operation == 'not_in':
                    # Exclude rows where column value is in the specified values
                    filtered_df = filtered_df[~filtered_df[column].astype(str).isin([str(v) for v in values])]
                
                elif operation == 'contains':
                    # Include rows where column contains any of the specified values
                    pattern = '|'.join([re.escape(str(v)) for v in values])
                    filtered_df = filtered_df[filtered_df[column].astype(str).str.contains(pattern, case=False, na=False)]
                
                elif operation == 'not_contains':
                    # Exclude rows where column contains any of the specified values
                    pattern = '|'.join([re.escape(str(v)) for v in values])
                    filtered_df = filtered_df[~filtered_df[column].astype(str).str.contains(pattern, case=False, na=False)]
                
                elif operation == 'range':
                    # Numeric range filter (expects [min, max] in values)
                    if len(values) == 2:
                        min_val, max_val = values
                        filtered_df = filtered_df[
                            (pd.to_numeric(filtered_df[column], errors='coerce') >= min_val) &
                            (pd.to_numeric(filtered_df[column], errors='coerce') <= max_val)
                        ]
                
                elif operation == 'date_range':
                    # Date range filter (expects [start_date, end_date] in values)
                    if len(values) == 2:
                        start_date, end_date = pd.to_datetime(values)
                        date_column = pd.to_datetime(filtered_df[column], errors='coerce')
                        filtered_df = filtered_df[(date_column >= start_date) & (date_column <= end_date)]
                
                rows_after = len(filtered_df)
                rows_removed = rows_before - rows_after
                
                filter_results["filters_applied"].append({
                    "filter_name": filter_name,
                    "column": column,
                    "operation": operation,
                    "values": values,
                    "rows_before": rows_before,
                    "rows_after": rows_after,
                    "rows_removed": rows_removed
                })
            
            # Update tracking columns
            if 'filter_applied' in filtered_df.columns:
                filtered_df['filter_applied'] = True
                filtered_df['filter_timestamp'] = datetime.now().isoformat()
                filtered_df['filter_criteria'] = json.dumps(filter_config)
                filtered_df['last_updated'] = datetime.now().isoformat()
            
            filter_results["filtered_rows"] = len(filtered_df)
            filter_results["reduction_percentage"] = round(
                ((filter_results["original_rows"] - filter_results["filtered_rows"]) / 
                 filter_results["original_rows"]) * 100, 2
            ) if filter_results["original_rows"] > 0 else 0
            
            return filtered_df, filter_results
            
        except Exception as e:
            filter_results["success"] = False
            filter_results["warnings"].append(f"Filter error: {str(e)}")
            return df, filter_results  # Return original data on error
    
    def create_filter_config(self, df: pd.DataFrame, 
                           primary_column: str = None, primary_values: List[str] = None,
                           secondary_column: str = None, secondary_values: List[str] = None,
                           advanced_filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a comprehensive filter configuration from user inputs.
        
        Args:
            df: Source dataframe
            primary_column: Main filter column
            primary_values: Values for primary filter
            secondary_column: Secondary filter column
            secondary_values: Values for secondary filter
            advanced_filters: Additional complex filters
            
        Returns:
            Complete filter configuration dictionary
        """
        filter_config = {}
        
        # Primary filter
        if primary_column and primary_values and primary_column in df.columns:
            filter_config["primary_filter"] = {
                "enabled": True,
                "column": primary_column,
                "operation": "in",
                "values": primary_values,
                "description": f"Primary filter on {primary_column}"
            }
        
        # Secondary filter
        if secondary_column and secondary_values and secondary_column in df.columns:
            filter_config["secondary_filter"] = {
                "enabled": True,
                "column": secondary_column,
                "operation": "in",
                "values": secondary_values,
                "description": f"Secondary filter on {secondary_column}"
            }
        
        # Advanced filters
        if advanced_filters:
            for name, spec in advanced_filters.items():
                if spec.get('column') in df.columns:
                    filter_config[f"advanced_{name}"] = spec
        
        return filter_config
    
    def get_filterable_columns_info(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Get comprehensive information about columns suitable for filtering.
        
        Args:
            df: Source dataframe
            
        Returns:
            Dictionary with detailed column information
        """
        column_info = {}
        
        try:
            filterable_columns = get_filterable_columns_safe(df)
            
            for column in filterable_columns:
                unique_values = safe_unique_values(df, column, max_values=1000)
                
                col_info = {
                    "data_type": str(df[column].dtype),
                    "unique_count": df[column].nunique(),
                    "null_count": df[column].isnull().sum(),
                    "null_percentage": round((df[column].isnull().sum() / len(df)) * 100, 2),
                    "unique_values": unique_values[:100],  # Limit for UI
                    "sample_values": df[column].dropna().head(5).astype(str).tolist(),
                    "is_numeric": pd.api.types.is_numeric_dtype(df[column]),
                    "is_datetime": pd.api.types.is_datetime64_any_dtype(df[column]),
                    "recommended_operations": []
                }
                
                # Recommend filter operations based on data characteristics
                if col_info["unique_count"] <= 50:
                    col_info["recommended_operations"].extend(["in", "not_in"])
                
                if col_info["is_numeric"]:
                    col_info["recommended_operations"].extend(["range", "greater_than", "less_than"])
                
                if col_info["is_datetime"]:
                    col_info["recommended_operations"].append("date_range")
                
                if col_info["data_type"] == "object":
                    col_info["recommended_operations"].extend(["contains", "not_contains"])
                
                column_info[column] = col_info
            
            return column_info
            
        except Exception as e:
            st.warning(f"Error analyzing filterable columns: {str(e)}")
            return {}
    
    def export_stage_data(self, df: pd.DataFrame, session_id: str, stage: str, 
                         export_format: str = "csv", description: str = "") -> Tuple[bool, str, bytes]:
        """
        Export data for download with multiple format support.
        
        Args:
            df: DataFrame to export
            session_id: Current session ID  
            stage: Current workflow stage
            export_format: Export format (csv, excel, json)
            description: Optional description for filename
            
        Returns:
            Tuple of (success, filename, file_bytes)
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create filename
            if description:
                safe_desc = re.sub(r'[^\w\s-]', '', description).strip()[:20]
                safe_desc = re.sub(r'[-\s]+', '_', safe_desc)
                filename = f"export_{stage}_{safe_desc}_{timestamp}"
            else:
                filename = f"export_{stage}_{timestamp}"
            
            # Generate file bytes based on format
            if export_format.lower() == "csv":
                output = StringIO()
                df.to_csv(output, index=False)
                file_bytes = output.getvalue().encode('utf-8')
                filename += ".csv"
                
            elif export_format.lower() == "excel":
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name=f'{stage}_data', index=False)
                    
                    # Add metadata sheet
                    metadata_df = pd.DataFrame({
                        'Attribute': ['Session ID', 'Export Stage', 'Export Timestamp', 'Total Rows', 'Total Columns'],
                        'Value': [session_id, stage, timestamp, len(df), len(df.columns)]
                    })
                    metadata_df.to_excel(writer, sheet_name='metadata', index=False)
                    
                file_bytes = output.getvalue()
                filename += ".xlsx"
                
            elif export_format.lower() == "json":
                # Convert DataFrame to JSON
                json_data = df.to_json(orient='records', date_format='iso')
                file_bytes = json_data.encode('utf-8')
                filename += ".json"
                
            else:
                raise ValueError(f"Unsupported export format: {export_format}")
            
            # Save to session directory for backup
            session_manager.save_dataframe(df, session_id, filename, "exports")
            
            return True, filename, file_bytes
            
        except Exception as e:
            st.error(f"Error exporting data: {str(e)}")
            return False, "", b""
    
    def merge_research_data(self, working_df: pd.DataFrame, 
                           research_results: Dict[str, Any],
                           merge_strategy: str = "update") -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Merge web research results into the working dataframe with advanced strategies.
        
        Args:
            working_df: Current working dataframe
            research_results: Dictionary of research results by company
            merge_strategy: How to handle conflicts ("update", "append", "preserve")
            
        Returns:
            Tuple of (merged DataFrame, merge statistics)
        """
        merge_stats = {
            "companies_processed": 0,
            "companies_updated": 0,
            "companies_failed": 0,
            "new_columns_added": [],
            "conflicts_resolved": 0
        }
        
        try:
            merged_df = working_df.copy()
            original_columns = set(merged_df.columns)
            
            # Track companies processed
            for idx, row in merged_df.iterrows():
                company_name = str(row.get('Consignee Name', '')).strip()
                
                if company_name in research_results:
                    merge_stats["companies_processed"] += 1
                    result = research_results[company_name]
                    
                    try:
                        # Update research status
                        merged_df.at[idx, 'web_research_status'] = result.get('status', 'completed')
                        merged_df.at[idx, 'research_timestamp'] = result.get('timestamp', datetime.now().isoformat())
                        merged_df.at[idx, 'research_attempts'] = merged_df.at[idx, 'research_attempts'] + 1
                        
                        # Update contact information
                        contacts = result.get('contacts', {})
                        if contacts:
                            merged_df.at[idx, 'contact_details'] = json.dumps(contacts)
                            merged_df.at[idx, 'contact_emails'] = contacts.get('emails', '')
                            merged_df.at[idx, 'contact_phones'] = contacts.get('phones', '')
                        
                        # Update company information
                        company_info = result.get('company_info', {})
                        if company_info:
                            merged_df.at[idx, 'company_website'] = company_info.get('website', '')
                            merged_df.at[idx, 'company_industry'] = company_info.get('industry', '')
                        
                        # Update quality score
                        merged_df.at[idx, 'research_quality_score'] = result.get('quality_score', 0)
                        
                        # Add any additional fields from research
                        for field, value in result.items():
                            if field not in ['status', 'timestamp', 'contacts', 'company_info', 'quality_score']:
                                column_name = f'research_{field}'
                                if column_name not in merged_df.columns:
                                    merged_df[column_name] = ''
                                    merge_stats["new_columns_added"].append(column_name)
                                
                                # Handle conflicts based on strategy
                                existing_value = merged_df.at[idx, column_name]
                                if existing_value and str(existing_value) != str(value):
                                    merge_stats["conflicts_resolved"] += 1
                                    
                                    if merge_strategy == "update":
                                        merged_df.at[idx, column_name] = str(value)
                                    elif merge_strategy == "append":
                                        merged_df.at[idx, column_name] = f"{existing_value}; {value}"
                                    # "preserve" keeps existing value
                                else:
                                    merged_df.at[idx, column_name] = str(value)
                        
                        merged_df.at[idx, 'last_updated'] = datetime.now().isoformat()
                        merge_stats["companies_updated"] += 1
                        
                    except Exception as e:
                        st.warning(f"Error merging data for {company_name}: {str(e)}")
                        merge_stats["companies_failed"] += 1
            
            # Identify new columns
            new_columns = set(merged_df.columns) - original_columns
            merge_stats["new_columns_added"].extend(list(new_columns))
            
            return merged_df, merge_stats
            
        except Exception as e:
            st.error(f"Error merging research data: {str(e)}")
            return working_df, merge_stats
    
    def update_email_status(self, working_df: pd.DataFrame, 
                           email_results: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Update working dataframe with email campaign results and delivery tracking.
        
        Args:
            working_df: Current working dataframe
            email_results: Dictionary of email results by company
            
        Returns:
            Tuple of (updated DataFrame, update statistics)
        """
        update_stats = {
            "companies_processed": 0,
            "emails_sent": 0,
            "emails_failed": 0,
            "delivery_tracked": 0
        }
        
        try:
            updated_df = working_df.copy()
            
            for idx, row in updated_df.iterrows():
                company_name = str(row.get('Consignee Name', '')).strip()
                
                if company_name in email_results:
                    update_stats["companies_processed"] += 1
                    result = email_results[company_name]
                    
                    # Update email status
                    status = result.get('status', 'failed')
                    updated_df.at[idx, 'email_sent_status'] = status
                    updated_df.at[idx, 'email_timestamp'] = result.get('timestamp', datetime.now().isoformat())
                    updated_df.at[idx, 'email_campaign_id'] = result.get('campaign_id', '')
                    
                    if status == 'sent':
                        update_stats["emails_sent"] += 1
                    else:
                        update_stats["emails_failed"] += 1
                    
                    # Update delivery tracking if available
                    if 'delivery_status' in result:
                        updated_df.at[idx, 'email_delivery_status'] = result['delivery_status']
                        update_stats["delivery_tracked"] += 1
                    
                    if 'open_status' in result:
                        updated_df.at[idx, 'email_open_status'] = result['open_status']
                    
                    if 'click_status' in result:
                        updated_df.at[idx, 'email_click_status'] = result['click_status']
                    
                    if 'response_status' in result:
                        updated_df.at[idx, 'email_response_status'] = result['response_status']
                    
                    updated_df.at[idx, 'last_updated'] = datetime.now().isoformat()
            
            return updated_df, update_stats
            
        except Exception as e:
            st.error(f"Error updating email status: {str(e)}")
            return working_df, update_stats
    
    def calculate_processing_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate comprehensive processing statistics for the current dataframe.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with detailed processing statistics
        """
        try:
            stats = {
                "basic_stats": {
                    "total_rows": len(df),
                    "total_columns": len(df.columns),
                    "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
                },
                "data_quality": get_data_quality_score(df),
                "workflow_progress": {},
                "column_analysis": {}
            }
            
            # Workflow progress analysis (if tracking columns exist)
            if 'web_research_status' in df.columns:
                research_counts = df['web_research_status'].value_counts().to_dict()
                stats["workflow_progress"]["research"] = research_counts
            
            if 'email_sent_status' in df.columns:
                email_counts = df['email_sent_status'].value_counts().to_dict()
                stats["workflow_progress"]["email"] = email_counts
            
            if 'filter_applied' in df.columns:
                filtered_count = df['filter_applied'].sum()
                stats["workflow_progress"]["filtering"] = {
                    "filtered_rows": int(filtered_count),
                    "unfiltered_rows": int(len(df) - filtered_count)
                }
            
            # Column analysis for key columns
            key_columns = ['Consignee Name', 'contact_details', 'company_website']
            for col in key_columns:
                if col in df.columns:
                    stats["column_analysis"][col] = {
                        "non_null_count": int(df[col].notna().sum()),
                        "unique_count": int(df[col].nunique()),
                        "completion_rate": round((df[col].notna().sum() / len(df)) * 100, 2)
                    }
            
            return stats
            
        except Exception as e:
            st.warning(f"Error calculating processing stats: {str(e)}")
            return {"basic_stats": {"total_rows": len(df), "total_columns": len(df.columns)}}


# Global CSV processor instance
csv_processor = CSVProcessor()
