"""
Enhanced Data Loader Service  
===========================
Data loading, parsing, and persistence with session management.
STAGE 2: Session Management System Integration
"""
import pandas as pd
import streamlit as st
from io import BytesIO
from typing import Optional, Tuple, Dict, Any
import os
from datetime import datetime

from utils.data_utils import clean_dataframe_for_arrow
from state_management import add_data_checkpoint, update_stage_progress


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
# SESSION-AWARE DATA MANAGEMENT (STAGE 2)
# ============================================================================

def load_session_data(session_id: str) -> Optional[pd.DataFrame]:
    """
    Load working data for a session.
    
    Args:
        session_id: Session identifier
        
    Returns:
        DataFrame or None
    """
    try:
        from services.session_manager import session_manager
        
        # Try to load most recent upload stage data
        df = session_manager.load_stage_data(session_id, "upload")
        
        if df is not None:
            # Update session state
            st.session_state.working_data = df
            return df
        
        # Fallback: try to load current working data
        session_dir = f"temp_files/session_{session_id}"
        working_data_path = os.path.join(session_dir, "data", "working_data.csv")
        
        if os.path.exists(working_data_path):
            df = pd.read_csv(working_data_path)
            st.session_state.working_data = df
            return df
        
        return None
        
    except Exception as e:
        st.warning(f"Error loading session data: {str(e)}")
        return None


def save_session_data(data: pd.DataFrame, session_id: str, stage: str = "upload") -> bool:
    """
    Save data to session storage.
    
    Args:
        data: DataFrame to save
        session_id: Session identifier
        stage: Workflow stage
        
    Returns:
        bool: Success status
    """
    try:
        from services.session_manager import session_manager
        
        # Save data to session
        success = session_manager.save_stage_data(
            session_id, stage, data, f"Data saved for {stage} stage"
        )
        
        if success:
            # Update session state
            st.session_state.working_data = data
            
            # Add checkpoint
            add_data_checkpoint(f"Data saved to session for {stage} stage", data)
            
            # Update progress
            update_stage_progress(stage, True)
        
        return success
        
    except Exception as e:
        st.error(f"Error saving session data: {str(e)}")
        return False


def create_working_copy(original_data: pd.DataFrame, session_id: str, 
                       filename: str = "") -> str:
    """
    Create a working copy of original data with tracking columns.
    
    Args:
        original_data: Original DataFrame
        session_id: Session identifier
        filename: Original filename
        
    Returns:
        str: Working copy filename
    """
    try:
        from services.session_manager import session_manager
        
        # Create working copy with session manager
        working_filename = session_manager.create_working_copy(
            session_id, original_data, filename
        )
        
        if working_filename:
            # Load the working copy back
            working_data = load_session_data(session_id)
            
            if working_data is not None:
                # Add checkpoint
                add_data_checkpoint("Working copy created with tracking columns", working_data)
                
                return working_filename
        
        return ""
        
    except Exception as e:
        st.error(f"Error creating working copy: {str(e)}")
        return ""


def load_session_data_with_contacts(session_id: str) -> Optional[pd.DataFrame]:
    """
    Load data with contact information for email stage.
    
    Args:
        session_id: Session identifier
        
    Returns:
        DataFrame with contact information or None
    """
    try:
        from services.session_manager import session_manager
        
        # Try to load from map stage (with research results)
        df = session_manager.load_stage_data(session_id, "map")
        
        if df is not None:
            # Verify it has contact information
            if 'contact_found' in df.columns or 'research_status' in df.columns:
                st.session_state.working_data = df
                return df
        
        # Fallback: load from upload and warn about missing contacts
        df = load_session_data(session_id)
        if df is not None:
            st.warning("No contact information found. Please complete the research stage first.")
        
        return df
        
    except Exception as e:
        st.warning(f"Error loading session data with contacts: {str(e)}")
        return None


def export_stage_data(data: pd.DataFrame, stage: str, session_id: str, 
                     export_format: str = "csv") -> Tuple[bool, str]:
    """
    Export data for a specific stage.
    
    Args:
        data: DataFrame to export
        stage: Current workflow stage
        session_id: Session identifier
        export_format: Export format (csv, excel)
        
    Returns:
        Tuple of (success, file_path)
    """
    try:
        from services.session_manager import session_manager
        
        # Create export using session manager
        success, file_path = session_manager.create_export(
            session_id, stage, data, export_format
        )
        
        if success:
            # Add checkpoint for export
            add_data_checkpoint(f"Data exported for {stage} stage", data)
        
        return success, file_path
        
    except Exception as e:
        st.error(f"Error exporting stage data: {str(e)}")
        return False, ""


def get_session_data_summary(session_id: str) -> Dict[str, Any]:
    """
    Get comprehensive summary of session data.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Dictionary with session data information
    """
    try:
        from services.session_manager import session_manager
        
        # Get session summary
        session_summary = session_manager.get_session_summary(session_id)
        
        if session_summary.get("exists", False):
            # Add data-specific information
            working_data = load_session_data(session_id)
            
            data_info = {}
            if working_data is not None:
                data_info = {
                    "has_working_data": True,
                    "rows": len(working_data),
                    "columns": len(working_data.columns),
                    "column_names": list(working_data.columns),
                    "has_consignee_names": 'Consignee Name' in working_data.columns,
                    "has_tracking_columns": 'session_id' in working_data.columns
                }
            else:
                data_info = {"has_working_data": False}
            
            session_summary["data_info"] = data_info
        
        return session_summary
        
    except Exception as e:
        st.warning(f"Error getting session data summary: {str(e)}")
        return {"exists": False, "error": str(e)}


def restore_from_backup(session_id: str, stage: str) -> bool:
    """
    Restore data from backup for a specific stage.
    
    Args:
        session_id: Session identifier
        stage: Stage to restore
        
    Returns:
        bool: Success status
    """
    try:
        from services.session_manager import session_manager
        
        success = session_manager.restore_from_backup(session_id, stage)
        
        if success:
            # Reload working data
            restored_data = load_session_data(session_id)
            if restored_data is not None:
                add_data_checkpoint(f"Data restored from {stage} backup", restored_data)
        
        return success
        
    except Exception as e:
        st.error(f"Error restoring from backup: {str(e)}")
        return False


def merge_research_results(original_df: pd.DataFrame, 
                         research_results: Dict[str, Any],
                         session_id: str) -> pd.DataFrame:
    """
    Merge web research results into the original dataframe.
    
    Args:
        original_df: Original dataframe
        research_results: Research results dictionary
        session_id: Session identifier
        
    Returns:
        DataFrame with merged research results
    """
    try:
        merged_df = original_df.copy()
        
        # Add research results columns if they don't exist
        if 'research_status' not in merged_df.columns:
            merged_df['research_status'] = 'pending'
        
        if 'contact_found' not in merged_df.columns:
            merged_df['contact_found'] = False
        
        if 'contact_details' not in merged_df.columns:
            merged_df['contact_details'] = ''
        
        if 'research_timestamp' not in merged_df.columns:
            merged_df['research_timestamp'] = ''
        
        # Merge research results
        for idx, row in merged_df.iterrows():
            company_name = str(row.get('Consignee Name', ''))
            
            if company_name in research_results:
                result = research_results[company_name]
                
                merged_df.at[idx, 'research_status'] = result.get('status', 'completed')
                merged_df.at[idx, 'contact_found'] = result.get('contact_found', False)
                merged_df.at[idx, 'contact_details'] = result.get('contact_details', '')
                merged_df.at[idx, 'research_timestamp'] = datetime.now().isoformat()
        
        # Save merged data to session
        save_session_data(merged_df, session_id, "map")
        
        return merged_df
        
    except Exception as e:
        st.error(f"Error merging research results: {str(e)}")
        return original_df


def update_email_status(df: pd.DataFrame, email_results: Dict[str, Any],
                       session_id: str) -> pd.DataFrame:
    """
    Update dataframe with email campaign results.
    
    Args:
        df: Current dataframe
        email_results: Email results dictionary
        session_id: Session identifier
        
    Returns:
        DataFrame with updated email status
    """
    try:
        updated_df = df.copy()
        
        # Add email status columns if they don't exist
        if 'email_status' not in updated_df.columns:
            updated_df['email_status'] = 'not_sent'
        
        if 'email_timestamp' not in updated_df.columns:
            updated_df['email_timestamp'] = ''
        
        if 'email_delivery_status' not in updated_df.columns:
            updated_df['email_delivery_status'] = ''
        
        # Update email results
        for idx, row in updated_df.iterrows():
            company_name = str(row.get('Consignee Name', ''))
            
            if company_name in email_results:
                result = email_results[company_name]
                
                updated_df.at[idx, 'email_status'] = result.get('status', 'failed')
                updated_df.at[idx, 'email_timestamp'] = result.get('timestamp', '')
                updated_df.at[idx, 'email_delivery_status'] = result.get('delivery_status', '')
        
        # Save updated data to session
        save_session_data(updated_df, session_id, "analyze")
        
        return updated_df
        
    except Exception as e:
        st.error(f"Error updating email status: {str(e)}")
        return df
