"""
Railway Deployment Configuration
===============================
Configuration and utilities for Railway cloud deployment.
"""

import os
import streamlit as st
from typing import Dict, Any


class RailwayConfig:
    """Railway-specific configuration management."""
    
    def __init__(self):
        self.is_railway = self.detect_railway()
        self.environment = self.get_environment()
        self.memory_limit = self.get_memory_limit()
        self.storage_strategy = "memory" if self.is_railway else "disk"
    
    def detect_railway(self) -> bool:
        """Detect if running on Railway."""
        railway_indicators = [
            'RAILWAY_ENVIRONMENT',
            'RAILWAY_PROJECT_ID',
            'RAILWAY_SERVICE_ID',
            'RAILWAY_REPLICA_ID'
        ]
        
        return any(env_var in os.environ for env_var in railway_indicators)
    
    def get_environment(self) -> str:
        """Get deployment environment."""
        if self.is_railway:
            return os.environ.get('RAILWAY_ENVIRONMENT', 'production')
        return 'local'
    
    def get_memory_limit(self) -> int:
        """Get memory limit in MB."""
        if self.is_railway:
            # Railway typically provides 512MB-8GB
            # Default to conservative 256MB for data storage
            return int(os.environ.get('MEMORY_LIMIT_MB', '256'))
        return 1024  # Local development
    
    def get_config(self) -> Dict[str, Any]:
        """Get complete railway configuration."""
        return {
            'is_cloud': self.is_railway,
            'environment': self.environment,
            'memory_limit_mb': self.memory_limit,
            'storage_strategy': self.storage_strategy,
            'max_sessions': 5 if self.is_railway else 20,
            'session_timeout_hours': 1 if self.is_railway else 4,
            'max_file_size_mb': 25 if self.is_railway else 100,
            'auto_cleanup': True,
            'enable_debug': self.environment != 'production'
        }


# Global config instance
railway_config = RailwayConfig()


def get_railway_config() -> RailwayConfig:
    """Get Railway configuration instance."""
    return railway_config


def is_cloud_deployment() -> bool:
    """Check if running in cloud deployment."""
    return railway_config.is_railway


def get_deployment_info() -> Dict[str, Any]:
    """Get deployment information for debugging."""
    config = railway_config.get_config()
    
    if railway_config.is_railway:
        config.update({
            'railway_project_id': os.environ.get('RAILWAY_PROJECT_ID', 'unknown'),
            'railway_service_id': os.environ.get('RAILWAY_SERVICE_ID', 'unknown'),
            'railway_environment': os.environ.get('RAILWAY_ENVIRONMENT', 'unknown'),
            'port': os.environ.get('PORT', '8501'),
        })
    
    return config
