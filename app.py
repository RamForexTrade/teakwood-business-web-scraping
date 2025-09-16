"""
Business Research Tool
=====================
Main application entry point for the Streamlit business research tool.
Auto-detects cloud deployment and optimizes accordingly.
"""
# Load environment variables FIRST before any other imports
# Environment setup with fallbacks - only run once to prevent repeated messages
if 'env_loaded' not in globals():
    try:
        # Try simple direct loading first
        from simple_env import load_env_direct, verify_env
        load_env_direct()
        verify_env()
        globals()['env_loaded'] = True
    except ImportError:
        try:
            # Fallback to dotenv
            from dotenv import load_dotenv
            load_dotenv()
            print("✅ Using dotenv fallback")
            globals()['env_loaded'] = True
        except ImportError:
            # Manual fallback if nothing else works
            import os
            os.environ['GROQ_API_KEY'] = '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'
            os.environ['TAVILY_API_KEY'] = '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'
            print("✅ Using manual environment setting")
            globals()['env_loaded'] = True
    except Exception as e:
        print(f"⚠️ Environment loading error: {e}")
        # Final manual fallback
        import os
        os.environ['GROQ_API_KEY'] = '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'
        os.environ['TAVILY_API_KEY'] = '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'
        print("✅ Using final manual fallback")
        globals()['env_loaded'] = True

import streamlit as st
import os
import logging

# Health check for cloud deployment
if st.query_params.get("health") == "check":
    from health_check import *

# Auto-detect cloud deployment
is_cloud = any(env_var in os.environ for env_var in ['RAILWAY_ENVIRONMENT', 'RAILWAY_PROJECT_ID'])

# Import appropriate modules based on deployment
# DEPLOYMENT FIX: Use regular state management for better navigation compatibility
from state_management import initialize_state, get_state

if is_cloud:
    from railway_config import get_railway_config
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Running in cloud mode - using standard session management for navigation compatibility")

from utils.layout import setup_page_config, render_navigation_sidebar, render_progress_indicator


def main():
    """Main application entry point."""
    # Setup
    setup_page_config()
    initialize_state()

    # Periodic cleanup to prevent temp file accumulation
    from state_management import periodic_cleanup
    periodic_cleanup()

    # CLOUD DEPLOYMENT DEBUG: Show deployment info in debug mode
    if is_cloud and st.session_state.get('show_debug', False):
        st.sidebar.success("🌐 Cloud Mode Active")
        st.sidebar.caption(f"Environment: {os.environ.get('RAILWAY_ENVIRONMENT', 'Unknown')}")

        # Manual garbage collection button for debugging
        if st.sidebar.button("🗑️ Manual Cleanup", help="Clean up temp files manually"):
            from state_management import comprehensive_garbage_collection
            cleanup_stats = comprehensive_garbage_collection()
            st.sidebar.success(f"Cleanup completed: {cleanup_stats}")

    # Get current state
    state = get_state()

    # CLOUD DEPLOYMENT FIX: Ensure current_stage is properly synchronized
    current_stage = getattr(state, 'current_stage', 'upload')
    if 'current_stage' in st.session_state:
        current_stage = st.session_state.current_stage
    elif hasattr(state, 'current_stage'):
        current_stage = state.current_stage
    else:
        current_stage = 'upload'

    # Render layout
    render_navigation_sidebar()
    render_progress_indicator()

    # Route to appropriate page based on current stage
    if current_stage == "upload":
        from pages.upload import render
        render()
    
    elif current_stage == "ai_chat":
        from pages.ai_chat import render
        render()

    elif current_stage == "visualizations":
        from pages.quick_visualizations import render
        render()

    elif current_stage == "map":
        # Use the business research page
        try:
            from pages.business_research import enhanced_business_research_page
            enhanced_business_research_page()
        except Exception as e:
            st.error(f"Error loading business research page: {str(e)}")
            st.error("Please try refreshing the page or contact support if the issue persists.")

            # Reset to upload stage as fallback
            if st.button("🔄 Return to Upload"):
                from controllers import go_to_stage
                go_to_stage("upload")

    elif current_stage == "analyze":
        from pages.email_outreach import render
        render()

    else:
        st.error(f"Unknown stage: {current_stage}")
        if st.button("Go to Upload"):
            from controllers import go_to_stage
            go_to_stage("upload")
    
    # Add company footer
    from utils.layout import render_winwood_footer
    render_winwood_footer()


if __name__ == "__main__":
    main()
