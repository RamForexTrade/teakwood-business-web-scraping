"""
Cloud Session Manager
=====================
Optimized session management for cloud deployments (Railway, etc.)
Uses in-memory storage and optional cloud persistence.
"""

import streamlit as st
import pandas as pd
import os
import json
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, List
import tempfile
import logging
from pathlib import Path
import hashlib
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CloudSessionManager:
    """
    Cloud-optimized session manager that minimizes disk usage
    and handles ephemeral storage limitations.
    """
    
    def __init__(self):
        # Use system temp directory for cloud compatibility
        self.base_temp_dir = tempfile.gettempdir()
        self.app_temp_dir = os.path.join(self.base_temp_dir, "streamlit_business_tool")
        
        # Ensure app temp directory exists
        os.makedirs(self.app_temp_dir, exist_ok=True)
        
        # Session limits for cloud deployment
        self.max_sessions = 10  # Limit concurrent sessions
        self.session_timeout = timedelta(hours=2)  # Auto-cleanup after 2 hours
        self.max_file_size_mb = 50  # Max file size to store
        
        # In-memory storage for active sessions
        if 'cloud_session_data' not in st.session_state:
            st.session_state.cloud_session_data = {}
        
        # Initialize cleanup
        self._cleanup_old_sessions()
    
    def create_session(self, session_id: Optional[str] = None) -> str:
        """Create a new cloud-optimized session."""
        if session_id is None:
            session_id = str(uuid.uuid4())
        
        # Cleanup old sessions if limit exceeded
        self._enforce_session_limits()
        
        # Create session metadata
        session_data = {
            'session_id': session_id,
            'created_at': datetime.now().isoformat(),
            'last_accessed': datetime.now().isoformat(),
            'data_stored_in_memory': True,
            'file_count': 0,
            'total_size_mb': 0
        }
        
        # Store in memory instead of disk
        st.session_state.cloud_session_data[session_id] = session_data
        
        logger.info(f"Created cloud session: {session_id}")
        return session_id
    
    def store_dataframe(self, session_id: str, df: pd.DataFrame, name: str = "main_data") -> bool:
        """Store dataframe in memory with size limits."""
        try:
            # Check dataframe size
            size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
            
            if size_mb > self.max_file_size_mb:
                logger.warning(f"DataFrame too large: {size_mb:.1f}MB > {self.max_file_size_mb}MB")
                return False
            
            # Store in session state instead of disk
            if session_id not in st.session_state.cloud_session_data:
                self.create_session(session_id)
            
            # Store dataframe in memory
            session_key = f"df_{session_id}_{name}"
            st.session_state[session_key] = df.copy()
            
            # Update session metadata
            session_data = st.session_state.cloud_session_data[session_id]
            session_data['last_accessed'] = datetime.now().isoformat()
            session_data['file_count'] = session_data.get('file_count', 0) + 1
            session_data['total_size_mb'] = session_data.get('total_size_mb', 0) + size_mb
            
            logger.info(f"Stored DataFrame '{name}' ({size_mb:.1f}MB) for session {session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing dataframe: {e}")
            return False
    
    def load_dataframe(self, session_id: str, name: str = "main_data") -> Optional[pd.DataFrame]:
        """Load dataframe from memory."""
        try:
            session_key = f"df_{session_id}_{name}"
            
            if session_key in st.session_state:
                # Update last accessed time
                if session_id in st.session_state.cloud_session_data:
                    st.session_state.cloud_session_data[session_id]['last_accessed'] = datetime.now().isoformat()
                
                df = st.session_state[session_key]
                logger.info(f"Loaded DataFrame '{name}' for session {session_id}")
                return df.copy()
            
            return None
            
        except Exception as e:
            logger.error(f"Error loading dataframe: {e}")
            return None
    
    def store_session_metadata(self, session_id: str, metadata: Dict[str, Any]) -> bool:
        """Store session metadata in memory."""
        try:
            if session_id not in st.session_state.cloud_session_data:
                self.create_session(session_id)
            
            # Merge with existing metadata
            session_data = st.session_state.cloud_session_data[session_id]
            session_data.update(metadata)
            session_data['last_accessed'] = datetime.now().isoformat()
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing session metadata: {e}")
            return False
    
    def load_session_metadata(self, session_id: str) -> Dict[str, Any]:
        """Load session metadata from memory."""
        if session_id in st.session_state.cloud_session_data:
            return st.session_state.cloud_session_data[session_id].copy()
        return {}
    
    def create_export_file(self, session_id: str, df: pd.DataFrame, filename: str) -> Optional[io.BytesIO]:
        """Create export file in memory instead of disk."""
        try:
            # Create in-memory file
            buffer = io.BytesIO()
            
            # Export based on file extension
            if filename.endswith('.csv'):
                df.to_csv(buffer, index=False)
            elif filename.endswith('.xlsx'):
                df.to_excel(buffer, index=False, engine='openpyxl')
            else:
                logger.error(f"Unsupported export format: {filename}")
                return None
            
            buffer.seek(0)
            logger.info(f"Created export file '{filename}' in memory for session {session_id}")
            return buffer
            
        except Exception as e:
            logger.error(f"Error creating export file: {e}")
            return None
    
    def cleanup_session(self, session_id: str) -> bool:
        """Clean up a specific session from memory."""
        try:
            # Remove session data
            if session_id in st.session_state.cloud_session_data:
                del st.session_state.cloud_session_data[session_id]
            
            # Remove dataframes
            keys_to_remove = []
            for key in st.session_state.keys():
                if key.startswith(f"df_{session_id}_"):
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del st.session_state[key]
            
            logger.info(f"Cleaned up session: {session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning up session: {e}")
            return False
    
    def _cleanup_old_sessions(self):
        """Clean up expired sessions."""
        try:
            current_time = datetime.now()
            sessions_to_remove = []
            
            for session_id, session_data in st.session_state.cloud_session_data.items():
                last_accessed = datetime.fromisoformat(session_data['last_accessed'])
                if current_time - last_accessed > self.session_timeout:
                    sessions_to_remove.append(session_id)
            
            for session_id in sessions_to_remove:
                self.cleanup_session(session_id)
                logger.info(f"Cleaned up expired session: {session_id}")
                
        except Exception as e:
            logger.error(f"Error during session cleanup: {e}")
    
    def _enforce_session_limits(self):
        """Enforce maximum session limits."""
        try:
            if len(st.session_state.cloud_session_data) >= self.max_sessions:
                # Remove oldest session
                oldest_session = min(
                    st.session_state.cloud_session_data.items(),
                    key=lambda x: x[1]['last_accessed']
                )
                self.cleanup_session(oldest_session[0])
                logger.info(f"Removed oldest session to enforce limits: {oldest_session[0]}")
                
        except Exception as e:
            logger.error(f"Error enforcing session limits: {e}")
    
    def get_session_stats(self) -> Dict[str, Any]:
        """Get statistics about current sessions."""
        try:
            stats = {
                'active_sessions': len(st.session_state.cloud_session_data),
                'max_sessions': self.max_sessions,
                'total_memory_mb': 0,
                'oldest_session': None,
                'newest_session': None
            }
            
            if st.session_state.cloud_session_data:
                # Calculate total memory usage
                total_size = sum(
                    session_data.get('total_size_mb', 0)
                    for session_data in st.session_state.cloud_session_data.values()
                )
                stats['total_memory_mb'] = round(total_size, 2)
                
                # Find oldest and newest sessions
                sessions_by_time = sorted(
                    st.session_state.cloud_session_data.items(),
                    key=lambda x: x[1]['created_at']
                )
                stats['oldest_session'] = sessions_by_time[0][1]['created_at']
                stats['newest_session'] = sessions_by_time[-1][1]['created_at']
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting session stats: {e}")
            return {'error': str(e)}
    
    def force_cleanup_all(self):
        """Force cleanup of all sessions - use with caution."""
        try:
            # Clear all cloud session data
            st.session_state.cloud_session_data = {}
            
            # Clear all dataframes
            keys_to_remove = []
            for key in st.session_state.keys():
                if key.startswith('df_'):
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del st.session_state[key]
            
            logger.info("Force cleanup completed - all sessions cleared")
            
        except Exception as e:
            logger.error(f"Error during force cleanup: {e}")


# Global instance
cloud_session_manager = CloudSessionManager()


def get_cloud_session_manager() -> CloudSessionManager:
    """Get the global cloud session manager instance."""
    return cloud_session_manager
