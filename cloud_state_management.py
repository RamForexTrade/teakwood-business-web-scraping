"""
Cloud State Management Module
============================
Cloud-optimized state management for Railway deployment.
Uses in-memory storage with minimal disk usage.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import streamlit as st
import pandas as pd
import uuid
import json
from datetime import datetime
import logging

# Import cloud session manager
from services.cloud_session_manager import get_cloud_session_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CloudAppState:
    """Cloud-optimized application state using dataclass."""
    
    # Session Management - Cloud Optimized
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    session_created: str = field(default_factory=lambda: datetime.now().isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    cloud_mode: bool = True  # Flag for cloud deployment
    
    # Navigation state
    current_stage: str = "upload"
    
    # Stage Progress Tracking
    stage_progress: Dict[str, bool] = field(default_factory=lambda: {
        'upload': False,
        'map': False, 
        'analyze': False
    })
    
    # Data state - References only (actual data in cloud session manager)
    uploaded_filename: str = ""
    data_loaded: bool = False
    data_size_mb: float = 0.0
    data_rows: int = 0
    data_columns: int = 0
    
    # Filter state
    filters_applied: Dict[str, Any] = field(default_factory=dict)
    primary_filter_column: str = ""
    primary_filter_values: list = field(default_factory=list)
    secondary_filter_column: str = ""
    secondary_filter_values: list = field(default_factory=list)
    
    # Analysis state
    analysis_complete: bool = False
    results_generated: bool = False
    
    # Email state
    email_config: Dict[str, Any] = field(default_factory=dict)
    emails_sent: int = 0
    
    # UI state
    show_debug: bool = False
    
    # Cloud-specific settings
    memory_limit_mb: float = 100.0  # Memory limit for this session
    auto_cleanup: bool = True
    
    def to_dict(self) -> dict:
        """Convert state to dictionary for persistence."""
        return {
            'session_id': self.session_id,
            'session_created': self.session_created,
            'last_updated': self.last_updated,
            'cloud_mode': self.cloud_mode,
            'current_stage': self.current_stage,
            'stage_progress': self.stage_progress,
            'uploaded_filename': self.uploaded_filename,
            'data_loaded': self.data_loaded,
            'data_size_mb': self.data_size_mb,
            'data_rows': self.data_rows,
            'data_columns': self.data_columns,
            'filters_applied': self.filters_applied,
            'primary_filter_column': self.primary_filter_column,
            'primary_filter_values': self.primary_filter_values,
            'secondary_filter_column': self.secondary_filter_column,
            'secondary_filter_values': self.secondary_filter_values,
            'analysis_complete': self.analysis_complete,
            'results_generated': self.results_generated,
            'email_config': self.email_config,
            'emails_sent': self.emails_sent,
            'show_debug': self.show_debug,
            'memory_limit_mb': self.memory_limit_mb,
            'auto_cleanup': self.auto_cleanup
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'CloudAppState':
        """Create state from dictionary."""
        return cls(**data)


# Global state key
CLOUD_STATE_KEY = "cloud_app_state"


def initialize_cloud_state() -> None:
    """Initialize cloud-optimized application state."""
    if CLOUD_STATE_KEY not in st.session_state:
        # Create new cloud state
        state = CloudAppState()
        st.session_state[CLOUD_STATE_KEY] = state
        
        # Initialize cloud session manager
        cloud_manager = get_cloud_session_manager()
        session_id = cloud_manager.create_session(state.session_id)
        
        # Store initial state metadata
        cloud_manager.store_session_metadata(session_id, state.to_dict())
        
        logger.info(f"Initialized cloud state with session: {session_id}")
    else:
        # Update timestamp for existing state
        state = st.session_state[CLOUD_STATE_KEY]
        state.last_updated = datetime.now().isoformat()


def get_cloud_state() -> CloudAppState:
    """Get current cloud application state."""
    if CLOUD_STATE_KEY not in st.session_state:
        initialize_cloud_state()
    
    return st.session_state[CLOUD_STATE_KEY]


def update_cloud_state(**kwargs) -> None:
    """Update cloud application state with new values."""
    state = get_cloud_state()
    
    # Update state attributes
    for key, value in kwargs.items():
        if hasattr(state, key):
            setattr(state, key, value)
        else:
            logger.warning(f"Attempted to set unknown state attribute: {key}")
    
    # Update timestamp
    state.last_updated = datetime.now().isoformat()
    
    # Persist to cloud session manager
    cloud_manager = get_cloud_session_manager()
    cloud_manager.store_session_metadata(state.session_id, state.to_dict())


def store_dataframe_in_cloud(df: pd.DataFrame, name: str = "main_data") -> bool:
    """Store dataframe using cloud session manager."""
    state = get_cloud_state()
    cloud_manager = get_cloud_session_manager()
    
    success = cloud_manager.store_dataframe(state.session_id, df, name)
    
    if success:
        # Update state metadata
        size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        update_cloud_state(
            data_loaded=True,
            data_size_mb=round(size_mb, 2),
            data_rows=len(df),
            data_columns=len(df.columns)
        )
        logger.info(f"Stored dataframe '{name}' ({size_mb:.1f}MB) in cloud session")
    
    return success


def load_dataframe_from_cloud(name: str = "main_data") -> Optional[pd.DataFrame]:
    """Load dataframe from cloud session manager."""
    state = get_cloud_state()
    cloud_manager = get_cloud_session_manager()
    
    df = cloud_manager.load_dataframe(state.session_id, name)
    
    if df is not None:
        logger.info(f"Loaded dataframe '{name}' from cloud session")
    
    return df


def get_main_dataframe() -> Optional[pd.DataFrame]:
    """Get the main dataframe from cloud storage."""
    return load_dataframe_from_cloud("main_data")


def set_main_dataframe(df: pd.DataFrame) -> bool:
    """Set the main dataframe in cloud storage."""
    return store_dataframe_in_cloud(df, "main_data")


def get_filtered_dataframe() -> Optional[pd.DataFrame]:
    """Get the filtered dataframe from cloud storage."""
    return load_dataframe_from_cloud("filtered_data")


def set_filtered_dataframe(df: pd.DataFrame) -> bool:
    """Set the filtered dataframe in cloud storage."""
    return store_dataframe_in_cloud(df, "filtered_data")


def create_export_download(df: pd.DataFrame, filename: str) -> Optional[bytes]:
    """Create export file for download."""
    state = get_cloud_state()
    cloud_manager = get_cloud_session_manager()
    
    buffer = cloud_manager.create_export_file(state.session_id, df, filename)
    
    if buffer:
        return buffer.getvalue()
    
    return None


def cleanup_cloud_session() -> None:
    """Clean up current cloud session."""
    state = get_cloud_state()
    cloud_manager = get_cloud_session_manager()
    
    success = cloud_manager.cleanup_session(state.session_id)
    
    if success:
        # Reset state
        del st.session_state[CLOUD_STATE_KEY]
        initialize_cloud_state()
        logger.info("Cloud session cleaned up successfully")


def get_cloud_session_stats() -> Dict[str, Any]:
    """Get cloud session statistics."""
    cloud_manager = get_cloud_session_manager()
    return cloud_manager.get_session_stats()


def force_cleanup_all_sessions() -> None:
    """Force cleanup of all sessions - Emergency use only."""
    cloud_manager = get_cloud_session_manager()
    cloud_manager.force_cleanup_all()
    
    # Reset current session
    if CLOUD_STATE_KEY in st.session_state:
        del st.session_state[CLOUD_STATE_KEY]
    
    initialize_cloud_state()
    logger.info("All cloud sessions force cleaned")


# Backward compatibility functions
def initialize_state() -> None:
    """Backward compatibility wrapper."""
    initialize_cloud_state()


def get_state() -> CloudAppState:
    """Backward compatibility wrapper."""
    return get_cloud_state()


def update_state(**kwargs) -> None:
    """Backward compatibility wrapper."""
    update_cloud_state(**kwargs)


# Enhanced state properties for cloud deployment
@property
def main_dataframe(state: CloudAppState) -> Optional[pd.DataFrame]:
    """Get main dataframe property."""
    return get_main_dataframe()


@property  
def filtered_dataframe(state: CloudAppState) -> Optional[pd.DataFrame]:
    """Get filtered dataframe property."""
    return get_filtered_dataframe()


# Add properties to CloudAppState
CloudAppState.main_dataframe = main_dataframe
CloudAppState.filtered_dataframe = filtered_dataframe
