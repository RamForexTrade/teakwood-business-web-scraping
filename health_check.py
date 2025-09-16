"""
Health Check Endpoint for Railway
=================================
Simple health check for cloud deployment monitoring.
"""

import streamlit as st
from datetime import datetime
from railway_config import get_railway_config, get_deployment_info

# Only create health check in cloud deployment
config = get_railway_config()

if config.is_railway and st.query_params.get("health") == "check":
    # Simple health check response
    health_data = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "deployment": "railway",
        "environment": config.environment,
        "memory_limit_mb": config.memory_limit,
        "storage_strategy": config.storage_strategy
    }
    
    st.json(health_data)
    st.stop()
