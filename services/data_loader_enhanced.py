"""
Enhanced Data Loader Service
===========================
Data loading, parsing, and persistence with session management for web scraping workflow.
"""
import pandas as pd
import streamlit as st
from io import BytesIO
from typing import Optional, Tuple, Dict, Any
import os
from datetime import datetime

from utils.data_utils import clean_dataframe_for_arrow
from services.session_manager import session_manager
from state_management import get_state, add_data_checkpoint, save_session_metadata


@st.cache_data(show_spinner="Loading CSV...")
def load_csv(file_bytes: bytes) -> pd.DataFrame:
    """Load CSV data from uploaded bytes. Cached for speed."""
    try:
        # Try different encodings if UTF-8 fails
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                # Convert bytes to BytesIO for pandas
                file_io = BytesIO(file_bytes)
                df = pd.read_csv(file_io, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                if encoding == encodings[-1]:  # Last encoding attempt
                    raise e
                continue
        
        if df is None:
            raise ValueError("Could not decode file with any supported encoding")
        
        # Basic validation
        if df.empty:
            st.warning("CSV file is empty")
            return pd.DataFrame()
        
        if len(df.columns) == 0:
            st.error("CSV file has no columns")
            return pd.DataFrame()
        
        # Clean for Arrow compatibility
        cleaned_df = clean_dataframe_for_arrow(df)
        
        return cleaned_df
        
    except Exception as e:
        st.error(f"Error loading CSV: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def load_from_path(path: str) -> pd.DataFrame:
    """Load CSV from a known path."""
    try:
        df = pd.read_csv(path)
        cleaned_df = clean_dataframe_for_arrow(df)
        return cleaned_df
    except Exception as e:
        st.error(f"Error loading CSV from path: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def get_sample_data() -> pd.DataFrame:
    """Generate or load sample dataset for demo/testing."""
    df = pd.DataFrame({
        "Consignee Name": ["ABC Corp", "XYZ Ltd", "Global Industries"],
        "Product": ["Electronics", "Textiles", "Machinery"], 
        "Country": ["USA", "UK", "Germany"],
        "Value": [10000, 25000, 50000],
    })
    return clean_dataframe_for_arrow(df)


# ============================================================================
# SESSION-AWARE DATA MANAGEMENT (NEW)
# ============================================================================

def initialize_session_data(uploaded_file, filename: str) -> Tuple[bool, str]:
    """Initialize session with uploaded data and create working copy."""
    try:
        state = get_state()
        
        # Load the uploaded file
        file_bytes = uploaded_file.getvalue()
        df = load_csv(file_bytes)
        
        if df.empty:
            return False, "Failed to load CSV data"
        
        # Store original data in state
        state.original_dataframe = df.copy()
        state.main_dataframe = df.copy()
        state.uploaded_filename = filename
        state.uploaded_file = uploaded_file
        
        # Create session structure
        session_manager.create_session_structure(state.session_id)
        
        # Save original data
        original_saved = session_manager.save_dataframe(
            df, state.session_id, f"original_{filename}", "backups"
        )
        
        # Create working copy with tracking columns
        working_success, working_filename = session_manager.create_working_copy(
            df, state.session_id, filename
        )
        
        if working_success:
            # Load the working copy back into state
            working_df = session_manager.load_dataframe(
                state.session_id, working_filename, "data"
            )
            if working_df is not None:
                state.working_data = working_df
            
            # Add data checkpoint
            add_data_checkpoint("File uploaded and session initialized", df)
            
            # Save session metadata
            save_session_metadata(state)
            
            success_msg = f"Session initialized successfully. Session ID: {state.session_id[:8]}..."
            return True, success_msg
        else:
            return False, "Failed to create working copy"
            
    except Exception as e:
        st.error(f"Error initializing session data: {str(e)}")
        return False, str(e)


def load_session_data(session_id: str) -> Optional[pd.DataFrame]:
    """Load the current working data for a session."""
    try:
        state = get_state()
        
        # First try to get from current state
        if state.working_data is not None and state.session_id == session_id:
            return state.working_data
        
        # Otherwise load from file system
        # Try to find the most recent working file
        files_info = session_manager.get_session_files(session_id)
        data_files = files_info.get('data', [])
        
        working_files = [f for f in data_files if f['filename'].startswith('working_')]
        
        if working_files:
            # Get the most recent working file
            working_files.sort(key=lambda x: x['modified'], reverse=True)
            latest_file = working_files[0]
            
            # Extract filename without timestamp prefix
            filename = latest_file['filename']
            
            df = session_manager.load_dataframe(session_id, filename, "data")
            
            if df is not None:
                # Update state
                state.working_data = df
                return df
        
        return None
        
    except Exception as e:
        st.warning(f"Error loading session data: {str(e)}")
        return None


def save_session_data(df: pd.DataFrame, session_id: str, stage: str, 
                     description: str = "") -> bool:
    """Save current working data to session storage."""
    try:
        state = get_state()
        
        # Update working data in state
        state.working_data = df.copy()
        
        # Determine working filename
        if state.uploaded_filename:
            working_filename = f"working_{state.uploaded_filename}"
        else:
            working_filename = f"working_data_{stage}.csv"
        
        # Update working data in file system
        success = session_manager.update_working_data(
            session_id, working_filename, df, stage
        )
        
        if success:
            # Add data checkpoint
            checkpoint_desc = description or f"Data saved for {stage} stage"
            add_data_checkpoint(checkpoint_desc, df)
            
            # Save session metadata
            save_session_metadata(state)
        
        return success
        
    except Exception as e:
        st.error(f"Error saving session data: {str(e)}")
        return False


def export_data_for_download(df: pd.DataFrame, stage: str, 
                            description: str = "") -> Tuple[bool, str]:
    """Export data for user download."""
    try:
        state = get_state()
        
        success, file_path = session_manager.export_stage_data(
            state.session_id, df, stage, description
        )
        
        if success:
            # Add checkpoint for export
            add_data_checkpoint(f"Data exported for {stage}", df)
            return True, file_path
        else:
            return False, ""
            
    except Exception as e:
        st.error(f"Error exporting data: {str(e)}")
        return False, ""


def validate_session_data(df: pd.DataFrame, stage: str) -> Dict[str, Any]:
    """Validate data for a specific stage."""
    try:
        # Define required columns for each stage
        stage_requirements = {
            'upload': [],  # No specific requirements for upload
            'map': ['Consignee Name'],  # Need company names for research
            'analyze': ['Consignee Name', 'contact_details']  # Need contacts for email
        }
        
        expected_columns = stage_requirements.get(stage, [])
        
        # Use session manager validation
        validation_result = session_manager.validate_data_integrity(df, expected_columns)
        
        # Add stage-specific validations
        if stage == 'map':
            # Check if we have company names to research
            if 'Consignee Name' in df.columns:
                company_count = df['Consignee Name'].dropna().nunique()
                validation_result['info']['unique_companies'] = company_count
                
                if company_count == 0:
                    validation_result['errors'].append("No company names found for research")
                    validation_result['is_valid'] = False
            
        elif stage == 'analyze':
            # Check if we have contact details for email
            if 'contact_details' in df.columns:
                contacts_count = df['contact_details'].dropna().count()
                validation_result['info']['contacts_found'] = contacts_count
                
                if contacts_count == 0:
                    validation_result['warnings'].append("No contact details found for email campaign")
        
        return validation_result
        
    except Exception as e:
        return {
            "is_valid": False,
            "errors": [f"Validation error: {str(e)}"],
            "warnings": [],
            "info": {}
        }


def get_session_data_summary(session_id: str) -> Dict[str, Any]:
    """Get a summary of all data in a session."""
    try:
        # Get session summary from session manager
        session_summary = session_manager.get_session_summary(session_id)
        
        if not session_summary.get('exists', False):
            return {"exists": False}
        
        # Get current working data info
        working_data = load_session_data(session_id)
        data_info = {}
        
        if working_data is not None:
            validation = validate_session_data(working_data, 'upload')
            data_info = {
                "has_working_data": True,
                "row_count": len(working_data),
                "column_count": len(working_data.columns),
                "columns": list(working_data.columns),
                "validation": validation
            }
        else:
            data_info = {"has_working_data": False}
        
        # Combine session and data info
        return {
            **session_summary,
            "data_info": data_info
        }
        
    except Exception as e:
        st.warning(f"Error getting session data summary: {str(e)}")
        return {"exists": False, "error": str(e)}


def merge_research_data(working_df: pd.DataFrame, 
                       research_results: Dict[str, Any]) -> pd.DataFrame:
    """Merge web research results into working dataframe."""
    try:
        merged_df = working_df.copy()
        
        # Update research status and results
        for idx, row in merged_df.iterrows():
            company_name = row.get('Consignee Name', '')
            
            if company_name in research_results:
                result = research_results[company_name]
                
                # Update contact details
                merged_df.at[idx, 'contact_details'] = str(result.get('contacts', ''))
                merged_df.at[idx, 'web_research_status'] = 'completed'
                merged_df.at[idx, 'research_timestamp'] = datetime.now().isoformat()
                
                # Add any additional research data
                if 'website' in result:
                    if 'website' not in merged_df.columns:
                        merged_df['website'] = ''
                    merged_df.at[idx, 'website'] = result['website']
                
                if 'industry' in result:
                    if 'industry' not in merged_df.columns:
                        merged_df['industry'] = ''
                    merged_df.at[idx, 'industry'] = result['industry']
        
        return merged_df
        
    except Exception as e:
        st.error(f"Error merging research data: {str(e)}")
        return working_df  # Return original if merge fails


def update_email_status(working_df: pd.DataFrame, 
                       email_results: Dict[str, Any]) -> pd.DataFrame:
    """Update working dataframe with email campaign results."""
    try:
        updated_df = working_df.copy()
        
        # Update email status based on results
        for idx, row in updated_df.iterrows():
            company_name = row.get('Consignee Name', '')
            
            if company_name in email_results:
                result = email_results[company_name]
                
                updated_df.at[idx, 'email_sent_status'] = result.get('status', 'failed')
                updated_df.at[idx, 'email_timestamp'] = result.get('timestamp', '')
                updated_df.at[idx, 'campaign_id'] = result.get('campaign_id', '')
                
                # Add delivery status if available
                if 'delivery_status' in result:
                    if 'email_delivery_status' not in updated_df.columns:
                        updated_df['email_delivery_status'] = ''
                    updated_df.at[idx, 'email_delivery_status'] = result['delivery_status']
        
        return updated_df
        
    except Exception as e:
        st.error(f"Error updating email status: {str(e)}")
        return working_df  # Return original if update fails
