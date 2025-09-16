"""
State Management Module
======================
Clean state management using dataclasses for explicit state structure.
Enhanced with session-based data persistence for web scraping workflow.
STAGE 2: Session Management System Implementation
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import streamlit as st
import pandas as pd
import uuid
import json
from datetime import datetime
import os

@dataclass
class AppState:
    """Main application state using dataclass for clean structure."""
    
    # Session Management
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    session_created: str = field(default_factory=lambda: datetime.now().isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Navigation state
    current_stage: str = "upload"
    
    # Stage Progress Tracking
    stage_progress: Dict[str, bool] = field(default_factory=lambda: {
        'upload': False,
        'map': False, 
        'analyze': False
    })
    
    # Data state (Enhanced with session persistence)
    uploaded_file: Optional[Any] = None
    uploaded_filename: str = ""
    original_dataframe: Optional[pd.DataFrame] = None  # Backup of original
    main_dataframe: Optional[pd.DataFrame] = None
    filtered_dataframe: Optional[pd.DataFrame] = None
    working_data: Optional[pd.DataFrame] = None  # Current working dataset
    
    # Data History for Undo/Resume functionality
    data_history: List[Dict[str, Any]] = field(default_factory=list)
    
    # Filter state (Enhanced)
    filters_applied: Dict[str, Any] = field(default_factory=dict)
    primary_filter_column: str = ""
    primary_filter_values: list = field(default_factory=list)
    secondary_filter_column: str = ""
    secondary_filter_values: list = field(default_factory=list)
    
    # Web Research state (NEW)
    research_results: Dict[str, Any] = field(default_factory=dict)
    research_progress: Dict[str, Any] = field(default_factory=dict)
    companies_to_research: List[str] = field(default_factory=list)
    
    # Email Campaign state (NEW)
    email_results: Dict[str, Any] = field(default_factory=dict)
    email_templates: Dict[str, str] = field(default_factory=dict)
    email_campaign_config: Dict[str, Any] = field(default_factory=dict)
    
    # Analysis state
    analysis_results: Dict[str, Any] = field(default_factory=dict)
    
    # UI state
    sidebar_expanded: bool = True
    show_debug: bool = False


def initialize_state() -> None:
    """Initialize session state with default AppState if not exists."""
    if "app_state" not in st.session_state:
        st.session_state.app_state = AppState()

        # COMPREHENSIVE GARBAGE COLLECTION: Clean up all temp files when new session starts
        try:
            cleanup_stats = comprehensive_garbage_collection()
            total_cleaned = sum(cleanup_stats.values())
            if total_cleaned > 0:
                print(f"🗑️ Garbage collection completed: {cleanup_stats}")
        except Exception:
            # Silently handle cleanup errors
            pass

        # Create session directories (skip in cloud to avoid permission issues)
        try:
            create_session_directories(st.session_state.app_state.session_id)
        except Exception:
            # Skip directory creation in cloud deployment
            pass

    # Initialize session-based state management
    initialize_session_state()

    # CLOUD DEPLOYMENT FIX: Ensure navigation state is properly initialized
    if 'current_stage' not in st.session_state:
        st.session_state.current_stage = st.session_state.app_state.current_stage


def initialize_session_state() -> None:
    """Initialize session state for the web scraping workflow."""
    if 'session_id' not in st.session_state:
        st.session_state.session_id = generate_session_id()
    
    if 'working_data' not in st.session_state:
        st.session_state.working_data = None
    
    if 'stage_progress' not in st.session_state:
        st.session_state.stage_progress = {
            'upload': False, 
            'map': False, 
            'analyze': False
        }
    
    if 'data_history' not in st.session_state:
        st.session_state.data_history = []
    
    if 'current_session_active' not in st.session_state:
        st.session_state.current_session_active = False
    
    if 'session_metadata' not in st.session_state:
        st.session_state.session_metadata = {}
    
    if 'workflow_state' not in st.session_state:
        st.session_state.workflow_state = "initialized"


def get_state() -> AppState:
    """Get current application state."""
    initialize_state()
    return st.session_state.app_state


def update_state(**kwargs) -> None:
    """Update state with new values."""
    state = get_state()
    for key, value in kwargs.items():
        if hasattr(state, key):
            setattr(state, key, value)
        else:
            raise ValueError(f"Invalid state attribute: {key}")


def reset_state() -> None:
    """Reset state to initial values."""
    st.session_state.app_state = AppState()


def get_state_summary() -> Dict[str, Any]:
    """Get a summary of current state for debugging."""
    state = get_state()
    return {
        "session_id": state.session_id,
        "session_created": state.session_created,
        "current_stage": state.current_stage,
        "stage_progress": state.stage_progress,
        "has_uploaded_file": state.uploaded_file is not None,
        "uploaded_filename": state.uploaded_filename,
        "has_dataframe": state.main_dataframe is not None,
        "dataframe_shape": state.main_dataframe.shape if state.main_dataframe is not None else None,
        "has_filtered_dataframe": state.filtered_dataframe is not None,
        "filtered_dataframe_shape": state.filtered_dataframe.shape if state.filtered_dataframe is not None else None,
        "has_working_data": state.working_data is not None,
        "working_data_shape": state.working_data.shape if state.working_data is not None else None,
        "primary_filter_column": state.primary_filter_column,
        "primary_filter_values_count": len(state.primary_filter_values),
        "secondary_filter_column": state.secondary_filter_column,
        "secondary_filter_values_count": len(state.secondary_filter_values),
        "research_companies_count": len(state.companies_to_research),
        "has_research_results": bool(state.research_results),
        "has_email_results": bool(state.email_results),
        "analysis_keys": list(state.analysis_results.keys()) if state.analysis_results else [],
        "data_history_length": len(state.data_history),
    }


# ============================================================================
# SESSION MANAGEMENT FUNCTIONS (REQUIRED)
# ============================================================================

def generate_session_id() -> str:
    """Generate a unique session identifier."""
    return str(uuid.uuid4())


def create_session_directories(session_id: str) -> None:
    """Create necessary directories for session data."""
    try:
        base_dir = "temp_files"
        session_dir = os.path.join(base_dir, f"session_{session_id}")
        
        # Create directories if they don't exist
        os.makedirs(session_dir, exist_ok=True)
        os.makedirs(os.path.join(session_dir, "data"), exist_ok=True)
        os.makedirs(os.path.join(session_dir, "exports"), exist_ok=True)
        os.makedirs(os.path.join(session_dir, "backups"), exist_ok=True)
    except Exception as e:
        # Silently handle directory creation errors to avoid breaking the app
        pass


def get_session_directory(session_id: str) -> str:
    """Get the directory path for a session."""
    return os.path.join("temp_files", f"session_{session_id}")


def save_session_metadata(state: AppState) -> bool:
    """Save session metadata to JSON file."""
    try:
        session_dir = get_session_directory(state.session_id)
        metadata_path = os.path.join(session_dir, "session_metadata.json")
        
        # Update last_updated timestamp
        state.last_updated = datetime.now().isoformat()
        
        metadata = {
            "session_id": state.session_id,
            "session_created": state.session_created,
            "last_updated": state.last_updated,
            "current_stage": state.current_stage,
            "stage_progress": state.stage_progress,
            "uploaded_filename": state.uploaded_filename,
            "filters_applied": state.filters_applied,
            "research_progress": state.research_progress,
            "email_campaign_config": state.email_campaign_config
        }
        
        # Ensure directory exists
        os.makedirs(session_dir, exist_ok=True)
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        return True
    except Exception as e:
        # Silently handle metadata save errors to avoid breaking the app
        return False


def load_session_metadata(session_id: str) -> Optional[Dict[str, Any]]:
    """Load session metadata from JSON file."""
    try:
        session_dir = get_session_directory(session_id)
        metadata_path = os.path.join(session_dir, "session_metadata.json")
        
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                return json.load(f)
        return None
    except Exception as e:
        return None


def update_stage_progress(stage: str, completed: bool = True) -> None:
    """Update progress for a specific stage."""
    try:
        state = get_state()
        state.stage_progress[stage] = completed
        state.last_updated = datetime.now().isoformat()
        save_session_metadata(state)
        
        # Also update Streamlit session state
        if 'stage_progress' in st.session_state:
            st.session_state.stage_progress[stage] = completed
    except Exception as e:
        # Silently handle errors to avoid breaking the app
        pass


def add_data_checkpoint(description: str, data: Optional[pd.DataFrame] = None) -> None:
    """Add a checkpoint to data history for undo functionality."""
    try:
        state = get_state()
        
        checkpoint = {
            "timestamp": datetime.now().isoformat(),
            "description": description,
            "stage": state.current_stage,
            "has_data": data is not None,
            "data_shape": data.shape if data is not None else None
        }
        
        state.data_history.append(checkpoint)
        
        # Keep only last 10 checkpoints to avoid memory issues
        if len(state.data_history) > 10:
            state.data_history = state.data_history[-10:]
        
        # Also update Streamlit session state
        if 'data_history' in st.session_state:
            st.session_state.data_history = state.data_history
    except Exception as e:
        # Silently handle errors to avoid breaking the app
        pass


def can_proceed_to_stage(target_stage: str) -> bool:
    """Check if user can proceed to a target stage based on progress."""
    try:
        state = get_state()
        
        stage_order = ['upload', 'map', 'analyze']
        
        if target_stage not in stage_order:
            return False
        
        target_index = stage_order.index(target_stage)
        
        # Check if all previous stages are completed
        for i in range(target_index):
            if not state.stage_progress[stage_order[i]]:
                return False
        
        return True
    except Exception as e:
        return True  # Allow progression if there's an error


def get_next_available_stage() -> str:
    """Get the next stage that the user can access."""
    try:
        state = get_state()
        
        if not state.stage_progress['upload']:
            return 'upload'
        elif not state.stage_progress['map']:
            return 'map'
        elif not state.stage_progress['analyze']:
            return 'analyze'
        else:
            return 'analyze'  # All stages complete, stay on analyze
    except Exception as e:
        return 'upload'  # Default to upload if there's an error


def cleanup_old_sessions(days_old: int = 7) -> int:
    """Clean up session directories older than specified days."""
    try:
        base_dir = "temp_files"
        if not os.path.exists(base_dir):
            return 0

        cutoff_time = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
        cleaned_count = 0

        for item in os.listdir(base_dir):
            item_path = os.path.join(base_dir, item)
            if os.path.isdir(item_path) and item.startswith("session_"):
                # Check if directory is old enough
                if os.path.getctime(item_path) < cutoff_time:
                    import shutil
                    shutil.rmtree(item_path)
                    cleaned_count += 1

        return cleaned_count
    except Exception as e:
        return 0


def comprehensive_garbage_collection() -> Dict[str, int]:
    """
    Comprehensive garbage collection for all temp files and directories.
    Returns statistics about what was cleaned up.
    """
    stats = {
        'session_dirs_cleaned': 0,
        'temp_files_cleaned': 0,
        'downloads_cleaned': 0,
        'cloud_sessions_cleaned': 0
    }

    try:
        # 1. Clean up session directories (aggressive - older than 6 hours)
        stats['session_dirs_cleaned'] = cleanup_old_sessions(days_old=0.25)  # 6 hours

        # 2. Clean up loose temp files and downloads
        temp_dirs = ["temp_files", "downloads", "exports", "templates"]
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                try:
                    for item in os.listdir(temp_dir):
                        item_path = os.path.join(temp_dir, item)
                        if os.path.isfile(item_path):
                            # Remove files older than 1 hour
                            if os.path.getctime(item_path) < (datetime.now().timestamp() - 3600):
                                os.remove(item_path)
                                stats['temp_files_cleaned'] += 1
                        elif os.path.isdir(item_path) and temp_dir == "temp_files":
                            # For temp_files directory, also clean subdirectories
                            try:
                                for subitem in os.listdir(item_path):
                                    subitem_path = os.path.join(item_path, subitem)
                                    if os.path.isfile(subitem_path):
                                        if os.path.getctime(subitem_path) < (datetime.now().timestamp() - 3600):
                                            os.remove(subitem_path)
                                            stats['temp_files_cleaned'] += 1
                            except Exception:
                                pass
                except Exception:
                    pass

        # 3. Clean up cloud session data if available
        try:
            if 'cloud_session_data' in st.session_state:
                from services.cloud_session_manager import get_cloud_session_manager
                cloud_manager = get_cloud_session_manager()
                cloud_manager._cleanup_old_sessions()
                stats['cloud_sessions_cleaned'] = 1
        except Exception:
            pass

        # 4. Clean up any CSV files in root directory (from exports/downloads)
        try:
            for item in os.listdir("."):
                if item.endswith(('.csv', '.xlsx', '.json')) and os.path.isfile(item):
                    # Remove export files older than 30 minutes
                    if os.path.getctime(item) < (datetime.now().timestamp() - 1800):
                        os.remove(item)
                        stats['temp_files_cleaned'] += 1
        except Exception:
            pass

        # 5. Clean up system temp directory (our app files only)
        try:
            import tempfile
            system_temp = tempfile.gettempdir()
            app_temp_dir = os.path.join(system_temp, "streamlit_business_tool")
            if os.path.exists(app_temp_dir):
                import shutil
                shutil.rmtree(app_temp_dir, ignore_errors=True)
                stats['temp_files_cleaned'] += 1
        except Exception:
            pass

    except Exception as e:
        print(f"⚠️ Garbage collection error: {e}")

    return stats


def periodic_cleanup() -> None:
    """
    Lightweight periodic cleanup that runs during session.
    Only cleans up very old files to prevent accumulation.
    """
    try:
        # Only run cleanup occasionally (every 50th call)
        if not hasattr(st.session_state, 'cleanup_counter'):
            st.session_state.cleanup_counter = 0

        st.session_state.cleanup_counter += 1

        # Run cleanup every 50 page loads
        if st.session_state.cleanup_counter % 50 == 0:
            # Quick cleanup of very old files (older than 2 hours)
            cutoff_time = datetime.now().timestamp() - 7200  # 2 hours

            temp_dirs = ["temp_files", "downloads"]
            for temp_dir in temp_dirs:
                if os.path.exists(temp_dir):
                    try:
                        for item in os.listdir(temp_dir):
                            item_path = os.path.join(temp_dir, item)
                            if os.path.isfile(item_path) and os.path.getctime(item_path) < cutoff_time:
                                os.remove(item_path)
                    except Exception:
                        pass

    except Exception:
        # Silently handle any errors
        pass


# ============================================================================
# SESSION WORKFLOW MANAGEMENT FUNCTIONS (NEW - STAGE 2)
# ============================================================================

def manage_stage_transitions(target_stage: str) -> bool:
    """
    Handle navigation between stages with validation.
    
    Args:
        target_stage: Stage to transition to
        
    Returns:
        bool: Whether transition is allowed
    """
    try:
        current_progress = st.session_state.get('stage_progress', {})
        
        # Define stage order and prerequisites
        stage_order = ['upload', 'map', 'analyze']
        
        if target_stage not in stage_order:
            st.error(f"Invalid stage: {target_stage}")
            return False
        
        target_index = stage_order.index(target_stage)
        
        # Check if all previous stages are completed
        for i in range(target_index):
            prev_stage = stage_order[i]
            if not current_progress.get(prev_stage, False):
                st.warning(f"Please complete {prev_stage} stage first")
                return False
        
        return True
        
    except Exception as e:
        st.error(f"Error in stage transition: {str(e)}")
        return False


def validate_stage_completion(stage: str) -> bool:
    """
    Ensure stage requirements are met before proceeding.
    
    Args:
        stage: Stage to validate
        
    Returns:
        bool: Whether stage is complete
    """
    try:
        if stage == "upload":
            # Check if data is uploaded and filtered
            return (st.session_state.get('working_data') is not None and
                    len(st.session_state.get('data_history', [])) > 0)
        
        elif stage == "map":
            # Check if research is completed
            return (st.session_state.get('working_data') is not None and
                    'research_results' in st.session_state and
                    st.session_state.get('research_results'))
        
        elif stage == "analyze":
            # Check if email campaign is configured
            return (st.session_state.get('working_data') is not None and
                    'email_results' in st.session_state)
        
        return False
        
    except Exception as e:
        st.warning(f"Error validating stage completion: {str(e)}")
        return False


def handle_session_restoration() -> bool:
    """
    Allow users to resume from any stage by restoring session data.
    
    Returns:
        bool: Whether session was successfully restored
    """
    try:
        from services.session_manager import session_manager
        
        if 'session_id' in st.session_state and st.session_state.session_id:
            session_id = st.session_state.session_id
            
            # Try to restore session
            if session_manager.session_exists(session_id):
                success = session_manager.load_session(session_id)
                
                if success:
                    st.session_state.current_session_active = True
                    st.success("Session restored successfully!")
                    
                    # Update workflow state based on stage progress
                    progress = st.session_state.get('stage_progress', {})
                    
                    if progress.get('analyze', False):
                        st.session_state.workflow_state = "email_completed"
                    elif progress.get('map', False):
                        st.session_state.workflow_state = "research_completed"
                    elif progress.get('upload', False):
                        st.session_state.workflow_state = "data_filtered"
                    else:
                        st.session_state.workflow_state = "initialized"
                    
                    return True
        
        return False
        
    except Exception as e:
        st.error(f"Error restoring session: {str(e)}")
        return False


def save_current_session_state() -> bool:
    """
    Save current Streamlit state to session storage.
    
    Returns:
        bool: Whether save was successful
    """
    try:
        from services.session_manager import session_manager
        
        if 'session_id' in st.session_state and st.session_state.session_id:
            return session_manager.save_session_state(st.session_state.session_id)
        
        return False
        
    except Exception as e:
        st.error(f"Error saving session state: {str(e)}")
        return False


def create_new_workflow_session() -> str:
    """
    Create a new workflow session and initialize it.
    
    Returns:
        str: New session ID
    """
    try:
        from services.session_manager import session_manager
        
        # Create new session
        new_session_id = session_manager.create_new_session()
        
        if new_session_id:
            # Update Streamlit state
            st.session_state.session_id = new_session_id
            st.session_state.current_session_active = True
            st.session_state.workflow_state = "initialized"
            
            # Reset progress
            st.session_state.stage_progress = {
                'upload': False,
                'map': False, 
                'analyze': False
            }
            
            # Clear data
            st.session_state.working_data = None
            st.session_state.data_history = []
            
            return new_session_id
        
        return ""
        
    except Exception as e:
        st.error(f"Error creating new session: {str(e)}")
        return ""


def get_workflow_status() -> Dict[str, Any]:
    """
    Get comprehensive workflow status information.
    
    Returns:
        Dict with workflow status details
    """
    try:
        return {
            "session_id": st.session_state.get('session_id', ''),
            "session_active": st.session_state.get('current_session_active', False),
            "workflow_state": st.session_state.get('workflow_state', 'unknown'),
            "stage_progress": st.session_state.get('stage_progress', {}),
            "has_working_data": st.session_state.get('working_data') is not None,
            "data_history_count": len(st.session_state.get('data_history', [])),
            "current_stage": get_current_active_stage(),
            "next_available_stage": get_next_available_stage()
        }
        
    except Exception as e:
        st.warning(f"Error getting workflow status: {str(e)}")
        return {"error": str(e)}


def get_current_active_stage() -> str:
    """
    Determine the current active stage based on progress.
    
    Returns:
        str: Current active stage
    """
    try:
        progress = st.session_state.get('stage_progress', {})
        
        if not progress.get('upload', False):
            return 'upload'
        elif not progress.get('map', False):
            return 'map'
        elif not progress.get('analyze', False):
            return 'analyze'
        else:
            return 'analyze'  # All stages complete
        
    except Exception as e:
        return 'upload'  # Default fallback
