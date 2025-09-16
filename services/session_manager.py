"""
Session Manager Service
======================
Comprehensive session lifecycle management for the Teakwood Business web scraping workflow.
Implements session-based data persistence, state restoration, and workflow progression tracking.
"""
import streamlit as st
import pandas as pd
import os
import json
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, List
import shutil
from pathlib import Path
import hashlib

from state_management import get_state, AppState


class SessionManager:
    """
    Manages session lifecycle, data persistence, and workflow state for the web scraping application.
    """
    
    def __init__(self):
        self.base_temp_dir = "temp_files"
        self.downloads_dir = "downloads"
        self.templates_dir = "templates"
        self.session_metadata_file = "session_metadata.json"
        self.ensure_directories()
    
    def ensure_directories(self) -> None:
        """Ensure all necessary directories exist."""
        try:
            for directory in [self.base_temp_dir, self.downloads_dir, self.templates_dir]:
                os.makedirs(directory, exist_ok=True)
        except Exception as e:
            st.warning(f"Directory creation warning: {str(e)}")
    
    # ========================================================================
    # SESSION LIFECYCLE MANAGEMENT
    # ========================================================================
    
    def create_new_session(self) -> str:
        """
        Create a new session with unique ID and directory structure.
        
        Returns:
            str: New session ID
        """
        session_id = str(uuid.uuid4())
        session_dir = self.get_session_directory(session_id)
        
        try:
            # Create session directory structure
            os.makedirs(session_dir, exist_ok=True)
            os.makedirs(os.path.join(session_dir, "data"), exist_ok=True)
            os.makedirs(os.path.join(session_dir, "exports"), exist_ok=True)
            os.makedirs(os.path.join(session_dir, "backups"), exist_ok=True)
            os.makedirs(os.path.join(session_dir, "research"), exist_ok=True)
            os.makedirs(os.path.join(session_dir, "emails"), exist_ok=True)
            
            # Initialize session metadata
            metadata = {
                "session_id": session_id,
                "created_at": datetime.now().isoformat(),
                "last_accessed": datetime.now().isoformat(),
                "stage_progress": {
                    "upload": False,
                    "map": False,
                    "analyze": False
                },
                "data_files": [],
                "workflow_state": "initialized",
                "user_actions": []
            }
            
            self.save_session_metadata(session_id, metadata)
            
            return session_id
            
        except Exception as e:
            st.error(f"Error creating new session: {str(e)}")
            return ""
    
    def get_session_directory(self, session_id: str) -> str:
        """Get the full path to a session directory."""
        return os.path.join(self.base_temp_dir, f"session_{session_id}")
    
    def session_exists(self, session_id: str) -> bool:
        """Check if a session exists."""
        session_dir = self.get_session_directory(session_id)
        return os.path.exists(session_dir) and os.path.exists(
            os.path.join(session_dir, self.session_metadata_file)
        )
    
    def load_session(self, session_id: str) -> bool:
        """
        Load an existing session into Streamlit state.
        
        Args:
            session_id: Session to load
            
        Returns:
            bool: Success status
        """
        if not self.session_exists(session_id):
            st.error(f"Session {session_id} does not exist")
            return False
        
        try:
            # Load session metadata
            metadata = self.load_session_metadata(session_id)
            if not metadata:
                return False
            
            # Update last accessed time
            metadata["last_accessed"] = datetime.now().isoformat()
            self.save_session_metadata(session_id, metadata)
            
            # Load session data into Streamlit state
            state = get_state()
            state.session_id = session_id
            state.stage_progress = metadata.get("stage_progress", {
                "upload": False, "map": False, "analyze": False
            })
            
            # Load working data if available
            working_data_path = os.path.join(
                self.get_session_directory(session_id), "data", "working_data.csv"
            )
            if os.path.exists(working_data_path):
                state.working_data = pd.read_csv(working_data_path)
            
            # Load original data if available
            original_data_path = os.path.join(
                self.get_session_directory(session_id), "backups", "original_data.csv"
            )
            if os.path.exists(original_data_path):
                state.original_dataframe = pd.read_csv(original_data_path)
                state.main_dataframe = state.original_dataframe.copy()
            
            st.success(f"Session {session_id[:8]}... loaded successfully")
            return True
            
        except Exception as e:
            st.error(f"Error loading session: {str(e)}")
            return False
    
    def save_session_state(self, session_id: str) -> bool:
        """
        Save current Streamlit state to session.
        
        Args:
            session_id: Session to save to
            
        Returns:
            bool: Success status
        """
        try:
            state = get_state()
            session_dir = self.get_session_directory(session_id)
            
            # Save working data
            if state.working_data is not None:
                working_data_path = os.path.join(session_dir, "data", "working_data.csv")
                state.working_data.to_csv(working_data_path, index=False)
            
            # Save original data
            if state.original_dataframe is not None:
                original_data_path = os.path.join(session_dir, "backups", "original_data.csv")
                state.original_dataframe.to_csv(original_data_path, index=False)
            
            # Update session metadata
            metadata = self.load_session_metadata(session_id) or {}
            metadata.update({
                "last_saved": datetime.now().isoformat(),
                "stage_progress": state.stage_progress,
                "uploaded_filename": state.uploaded_filename,
                "current_stage": state.current_stage
            })
            
            self.save_session_metadata(session_id, metadata)
            
            return True
            
        except Exception as e:
            st.error(f"Error saving session state: {str(e)}")
            return False
    
    # ========================================================================
    # DATA MANAGEMENT
    # ========================================================================
    
    def save_stage_data(self, session_id: str, stage: str, data: pd.DataFrame, 
                       description: str = "") -> bool:
        """
        Save data for a specific workflow stage.
        
        Args:
            session_id: Current session
            stage: Workflow stage (upload, map, analyze)
            data: DataFrame to save
            description: Optional description
            
        Returns:
            bool: Success status
        """
        try:
            session_dir = self.get_session_directory(session_id)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save to stage-specific directory
            stage_dir = os.path.join(session_dir, "data")
            filename = f"{stage}_data_{timestamp}.csv"
            file_path = os.path.join(stage_dir, filename)
            
            data.to_csv(file_path, index=False)
            
            # Update metadata
            metadata = self.load_session_metadata(session_id) or {}
            if "data_files" not in metadata:
                metadata["data_files"] = []
            
            metadata["data_files"].append({
                "stage": stage,
                "filename": filename,
                "timestamp": timestamp,
                "description": description,
                "rows": len(data),
                "columns": len(data.columns)
            })
            
            self.save_session_metadata(session_id, metadata)
            
            return True
            
        except Exception as e:
            st.error(f"Error saving stage data: {str(e)}")
            return False
    
    def load_stage_data(self, session_id: str, stage: str) -> Optional[pd.DataFrame]:
        """
        Load the most recent data for a workflow stage.
        
        Args:
            session_id: Current session
            stage: Workflow stage to load
            
        Returns:
            DataFrame or None
        """
        try:
            metadata = self.load_session_metadata(session_id)
            if not metadata or "data_files" not in metadata:
                return None
            
            # Find most recent file for the stage
            stage_files = [f for f in metadata["data_files"] if f["stage"] == stage]
            if not stage_files:
                return None
            
            # Get most recent file
            latest_file = max(stage_files, key=lambda x: x["timestamp"])
            file_path = os.path.join(
                self.get_session_directory(session_id), 
                "data", 
                latest_file["filename"]
            )
            
            if os.path.exists(file_path):
                return pd.read_csv(file_path)
            
            return None
            
        except Exception as e:
            st.warning(f"Error loading stage data: {str(e)}")
            return None
    
    def create_working_copy(self, session_id: str, original_data: pd.DataFrame, 
                          filename: str = "") -> str:
        """
        Create a working copy of original data with tracking columns.
        
        Args:
            session_id: Current session
            original_data: Original DataFrame
            filename: Original filename
            
        Returns:
            str: Working copy filename
        """
        try:
            # Add tracking columns
            working_data = original_data.copy()
            
            # Add session tracking columns
            tracking_columns = {
                'session_id': session_id,
                'created_at': datetime.now().isoformat(),
                'stage': 'upload',
                'processed': False,
                'research_status': 'pending',
                'contact_found': False,
                'email_status': 'not_sent'
            }
            
            for col, default_value in tracking_columns.items():
                working_data[col] = default_value
            
            # Save working copy
            working_filename = f"working_copy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            working_path = os.path.join(
                self.get_session_directory(session_id), 
                "data", 
                working_filename
            )
            
            working_data.to_csv(working_path, index=False)
            
            # Also save as current working data
            current_working_path = os.path.join(
                self.get_session_directory(session_id), 
                "data", 
                "working_data.csv"
            )
            working_data.to_csv(current_working_path, index=False)
            
            return working_filename
            
        except Exception as e:
            st.error(f"Error creating working copy: {str(e)}")
            return ""
    
    # ========================================================================
    # EXPORT AND DOWNLOAD MANAGEMENT
    # ========================================================================
    
    def create_export(self, session_id: str, stage: str, data: pd.DataFrame, 
                     export_type: str = "csv") -> Tuple[bool, str]:
        """
        Create an export file for download.
        
        Args:
            session_id: Current session
            stage: Current workflow stage
            data: Data to export
            export_type: Export format (csv, excel)
            
        Returns:
            Tuple of (success, file_path)
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{stage}_{timestamp}.{export_type}"
            
            # Create in downloads directory
            export_path = os.path.join(self.downloads_dir, filename)
            
            if export_type == "csv":
                data.to_csv(export_path, index=False)
            elif export_type == "excel":
                data.to_excel(export_path, index=False, engine='openpyxl')
            else:
                raise ValueError(f"Unsupported export type: {export_type}")
            
            # Also save to session exports
            session_export_path = os.path.join(
                self.get_session_directory(session_id), 
                "exports", 
                filename
            )
            
            if export_type == "csv":
                data.to_csv(session_export_path, index=False)
            elif export_type == "excel":
                data.to_excel(session_export_path, index=False, engine='openpyxl')
            
            return True, export_path
            
        except Exception as e:
            st.error(f"Error creating export: {str(e)}")
            return False, ""
    
    # ========================================================================
    # SESSION METADATA MANAGEMENT
    # ========================================================================
    
    def save_session_metadata(self, session_id: str, metadata: Dict[str, Any]) -> bool:
        """Save session metadata to JSON file."""
        try:
            session_dir = self.get_session_directory(session_id)
            metadata_path = os.path.join(session_dir, self.session_metadata_file)
            
            # Ensure directory exists
            os.makedirs(session_dir, exist_ok=True)
            
            # Update last modified time
            metadata["last_modified"] = datetime.now().isoformat()
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            return True
            
        except Exception as e:
            st.error(f"Error saving session metadata: {str(e)}")
            return False
    
    def load_session_metadata(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Load session metadata from JSON file."""
        try:
            session_dir = self.get_session_directory(session_id)
            metadata_path = os.path.join(session_dir, self.session_metadata_file)
            
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    return json.load(f)
            
            return None
            
        except Exception as e:
            st.warning(f"Error loading session metadata: {str(e)}")
            return None
    
    def update_stage_progress(self, session_id: str, stage: str, completed: bool = True) -> bool:
        """Update progress for a specific workflow stage."""
        try:
            metadata = self.load_session_metadata(session_id) or {}
            
            if "stage_progress" not in metadata:
                metadata["stage_progress"] = {"upload": False, "map": False, "analyze": False}
            
            metadata["stage_progress"][stage] = completed
            
            # Add user action log
            if "user_actions" not in metadata:
                metadata["user_actions"] = []
            
            metadata["user_actions"].append({
                "timestamp": datetime.now().isoformat(),
                "action": f"stage_{stage}_{'completed' if completed else 'started'}",
                "stage": stage
            })
            
            return self.save_session_metadata(session_id, metadata)
            
        except Exception as e:
            st.error(f"Error updating stage progress: {str(e)}")
            return False
    
    # ========================================================================
    # SESSION DISCOVERY AND MANAGEMENT
    # ========================================================================
    
    def list_sessions(self, max_age_days: int = 30) -> List[Dict[str, Any]]:
        """
        List all available sessions within age limit.
        
        Args:
            max_age_days: Maximum age of sessions to include
            
        Returns:
            List of session information dictionaries
        """
        sessions = []
        
        try:
            if not os.path.exists(self.base_temp_dir):
                return sessions
            
            cutoff_time = datetime.now() - timedelta(days=max_age_days)
            
            for item in os.listdir(self.base_temp_dir):
                if item.startswith("session_"):
                    session_id = item.replace("session_", "")
                    
                    metadata = self.load_session_metadata(session_id)
                    if metadata:
                        # Check age
                        created_at = datetime.fromisoformat(metadata.get("created_at", "1970-01-01"))
                        if created_at >= cutoff_time:
                            sessions.append({
                                "session_id": session_id,
                                "created_at": metadata.get("created_at"),
                                "last_accessed": metadata.get("last_accessed"),
                                "stage_progress": metadata.get("stage_progress", {}),
                                "workflow_state": metadata.get("workflow_state", "unknown"),
                                "data_files_count": len(metadata.get("data_files", []))
                            })
            
            # Sort by last accessed (most recent first)
            sessions.sort(key=lambda x: x.get("last_accessed", ""), reverse=True)
            
        except Exception as e:
            st.warning(f"Error listing sessions: {str(e)}")
        
        return sessions
    
    def cleanup_old_sessions(self, max_age_days: int = 30) -> int:
        """
        Clean up sessions older than specified days.
        
        Args:
            max_age_days: Sessions older than this will be deleted
            
        Returns:
            int: Number of sessions cleaned up
        """
        cleaned_count = 0
        
        try:
            if not os.path.exists(self.base_temp_dir):
                return 0
            
            cutoff_time = datetime.now() - timedelta(days=max_age_days)
            
            for item in os.listdir(self.base_temp_dir):
                if item.startswith("session_"):
                    session_path = os.path.join(self.base_temp_dir, item)
                    
                    # Check creation time
                    if os.path.getctime(session_path) < cutoff_time.timestamp():
                        shutil.rmtree(session_path, ignore_errors=True)
                        cleaned_count += 1
        
        except Exception as e:
            st.warning(f"Error during cleanup: {str(e)}")
        
        return cleaned_count
    
    def get_session_summary(self, session_id: str) -> Dict[str, Any]:
        """Get comprehensive summary of a session."""
        try:
            if not self.session_exists(session_id):
                return {"exists": False}
            
            metadata = self.load_session_metadata(session_id) or {}
            session_dir = self.get_session_directory(session_id)
            
            # Calculate directory size
            total_size = 0
            file_count = 0
            
            for dirpath, dirnames, filenames in os.walk(session_dir):
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(file_path)
                        file_count += 1
                    except (OSError, FileNotFoundError):
                        continue
            
            return {
                "exists": True,
                "session_id": session_id,
                "metadata": metadata,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "file_count": file_count,
                "directory_path": session_dir
            }
            
        except Exception as e:
            st.warning(f"Error getting session summary: {str(e)}")
            return {"exists": False, "error": str(e)}
    
    def restore_from_backup(self, session_id: str, stage: str) -> bool:
        """Restore data from a backup for a specific stage."""
        try:
            backup_dir = os.path.join(self.get_session_directory(session_id), "backups")
            backup_files = [f for f in os.listdir(backup_dir) if f.startswith(f"{stage}_")]
            
            if not backup_files:
                st.warning(f"No backup found for stage {stage}")
                return False
            
            # Get most recent backup
            latest_backup = max(backup_files)
            backup_path = os.path.join(backup_dir, latest_backup)
            
            # Restore to working data
            working_path = os.path.join(
                self.get_session_directory(session_id), 
                "data", 
                "working_data.csv"
            )
            
            shutil.copy2(backup_path, working_path)
            
            st.success(f"Restored from backup: {latest_backup}")
            return True
            
        except Exception as e:
            st.error(f"Error restoring from backup: {str(e)}")
            return False


# Global session manager instance
session_manager = SessionManager()


# ============================================================================
# STREAMLIT SESSION INTEGRATION FUNCTIONS
# ============================================================================

def initialize_session_on_upload() -> str:
    """
    Initialize or retrieve session ID for upload stage.
    
    Returns:
        str: Session ID
    """
    if 'session_id' not in st.session_state or not st.session_state.session_id:
        # Create new session
        session_id = session_manager.create_new_session()
        st.session_state.session_id = session_id
        st.success(f"New session created: {session_id[:8]}...")
    else:
        session_id = st.session_state.session_id
    
    return session_id


def save_filtered_data_to_session(filtered_data: pd.DataFrame) -> bool:
    """Save filtered data to current session."""
    if 'session_id' in st.session_state:
        return session_manager.save_stage_data(
            st.session_state.session_id, 
            "upload", 
            filtered_data, 
            "Filtered data from upload stage"
        )
    return False


def save_research_data_to_session(research_data: pd.DataFrame) -> bool:
    """Save research data to current session."""
    if 'session_id' in st.session_state:
        return session_manager.save_stage_data(
            st.session_state.session_id, 
            "map", 
            research_data, 
            "Research data from map stage"
        )
    return False


def load_session_data(session_id: str) -> Optional[pd.DataFrame]:
    """Load working data for a session."""
    return session_manager.load_stage_data(session_id, "upload")


def load_session_data_with_contacts(session_id: str) -> Optional[pd.DataFrame]:
    """Load data with contact information for email stage."""
    return session_manager.load_stage_data(session_id, "map")


def create_download_button(data: pd.DataFrame, filename_prefix: str, 
                          stage: str = "current") -> bool:
    """Create a download button for data export."""
    if 'session_id' in st.session_state:
        success, file_path = session_manager.create_export(
            st.session_state.session_id, 
            stage, 
            data, 
            "csv"
        )
        
        if success and os.path.exists(file_path):
            with open(file_path, 'r') as f:
                csv_data = f.read()
            
            st.download_button(
                label=f"Download {filename_prefix}.csv",
                data=csv_data,
                file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            return True
    
    return False
