"""
Business Research Page
=====================
AI-powered business research and contact information discovery.
"""

import streamlit as st
import pandas as pd
import time
import random
from datetime import datetime
from typing import Dict, List, Optional
import asyncio
import concurrent.futures

# Configure pandas to avoid dtype compatibility warnings
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')

# Add error handling for .str accessor issues
def safe_str_contains(series, pattern, **kwargs):
    """Safely use .str.contains() on a pandas Series that may contain non-string values."""
    try:
        # Ensure series is string type and handle NaN values
        str_series = series.fillna('').astype(str)
        return str_series.str.contains(pattern, **kwargs)
    except Exception as e:
        st.error(f"String operation error: {e}")
        return pd.Series([False] * len(series), index=series.index)


def enhanced_business_research_page():
    """Business Research page with AI-powered search functionality."""
    
    try:
        _enhanced_business_research_page_impl()
    except Exception as e:
        st.error(f"Error loading business research page: {str(e)}")
        st.error("Please try refreshing the page or contact support if the issue persists.")
        
        # Debug information for development
        if st.checkbox("Show debug information", value=False):
            st.exception(e)
        
        # Reset button
        if st.button("🔄 Return to Upload"):
            try:
                from controllers import go_to_stage
                go_to_stage('upload')
            except:
                if 'current_stage' in st.session_state:
                    st.session_state.current_stage = 'upload'
                st.rerun()


def _enhanced_business_research_page_impl():
    """Business Research page with AI-powered search functionality."""

    st.title("🔍 Business Research")
    st.markdown("**Step 2**: Research contact details for timber/wood businesses")
    st.info("✨ **Smart search** to find detailed business information and contact details!")
    
    # Initialize session state
    if 'research_results' not in st.session_state:
        st.session_state.research_results = {}
    
    if 'research_status' not in st.session_state:
        st.session_state.research_status = 'ready'
    
    if 'api_tested' not in st.session_state:
        st.session_state.api_tested = False
    
    # ENHANCED DATA LOADING - Prioritize enhanced data with research results
    data = None
    data_source = ""

    # Priority 1: ALWAYS use enhanced data if it exists (contains research results)
    if 'enhanced_data' in st.session_state and st.session_state.enhanced_data is not None:
        data = st.session_state.enhanced_data
        data_source = "enhanced data with research results"
        st.success("✅ **Using enhanced data with research results**")

        # Show research progress info
        if st.session_state.research_results:
            research_count = len(st.session_state.research_results)
            st.info(f"📊 **Research Status**: {research_count} companies researched")

    # Priority 2: Check if we have filtered data specifically for research
    elif ('filtered_data_for_research' in st.session_state and
          st.session_state.filtered_data_for_research is not None and
          st.session_state.get('research_uses_filtered_data', False)):
        data = st.session_state.filtered_data_for_research
        data_source = "filtered data from upload stage"

        # Show filter information
        if 'filter_info' in st.session_state:
            filter_info = st.session_state.filter_info
            st.info(f"🔍 **Using filtered dataset**: {filter_info['filtered_rows']:,} rows "
                   f"(filtered from {filter_info['original_rows']:,} original rows)")

            # Show filter details
            with st.expander("📋 Active Filter Details", expanded=False):
                if filter_info.get('primary_column'):
                    st.write(f"**Primary Filter**: {filter_info['primary_column']} ({len(filter_info.get('primary_values', []))} values)")
                if filter_info.get('secondary_column'):
                    st.write(f"**Secondary Filter**: {filter_info['secondary_column']} ({len(filter_info.get('secondary_values', []))} values)")
                st.write(f"**Filtered at**: {filter_info.get('filter_timestamp', 'Unknown')}")

    # Priority 3: Try working_data as fallback
    elif 'working_data' in st.session_state and st.session_state.working_data is not None:
        data = st.session_state.working_data
        data_source = "working data from session"
        st.info("📊 **Using working dataset from session**")
        
    # Priority 3: Check working_data from session state
    elif 'working_data' in st.session_state and st.session_state.working_data is not None:
        data = st.session_state.working_data
        data_source = "working data from session"
        st.info("📊 **Using working dataset from session**")
        
    # Priority 4: Fallback to state management
    else:
        try:
            from state_management import get_state
            state = get_state()
            
            # Check if we have filtered data in state management
            if (hasattr(state, 'filtered_dataframe') and state.filtered_dataframe is not None and 
                not state.filtered_dataframe.empty and
                (bool(state.primary_filter_values) or bool(state.secondary_filter_values))):
                data = state.filtered_dataframe
                data_source = "filtered data from state management"
                st.info("📊 **Using filtered data from state management**")
                
            # Final fallback to main dataframe
            elif hasattr(state, 'main_dataframe') and state.main_dataframe is not None:
                data = state.main_dataframe
                data_source = "main dataframe from state management"
                st.info("📊 **Using main dataset from state management**")
        except Exception as e:
            st.warning(f"⚠️ Could not load from state management: {e}")
    
    # Log data source for debugging
    if data is not None:
        st.caption(f"Data source: {data_source} | Shape: {data.shape}")
    
    # API Configuration Check
    with st.expander("🔧 API Configuration", expanded=not st.session_state.api_tested):
        st.write("**Required API Keys for AI Research:**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            tavily_key = st.text_input("Tavily API Key", 
                                     type="password", 
                                     help="Get your key from tavily.com",
                                     key="tavily_key_input")
        
        with col2:
            groq_key = st.text_input("Groq API Key", 
                                   type="password", 
                                   help="Get your key from console.groq.com",
                                   key="groq_key_input")
        
        if st.button("🧪 Test API Connection"):
            if tavily_key and groq_key:
                # Set environment variables for testing
                import os
                os.environ['TAVILY_API_KEY'] = tavily_key
                os.environ['GROQ_API_KEY'] = groq_key
                
                try:
                    from services.web_scraper import WebScraper
                    scraper = WebScraper()
                    
                    with st.spinner("Testing API connections..."):
                        api_ok, api_message = scraper.test_api_connection()
                    
                    if api_ok:
                        st.success(f"✅ {api_message}")
                        st.session_state.api_tested = True
                    else:
                        st.error(f"❌ {api_message}")
                except Exception as e:
                    st.error(f"❌ Configuration Error: {e}")
            else:
                st.warning("⚠️ Please enter both API keys")
        
        # Show setup instructions
        if not st.session_state.api_tested:
            st.info("""
            **Setup Instructions:**
            1. Get Tavily API key from [tavily.com](https://tavily.com) (for web search)
            2. Get Groq API key from [console.groq.com](https://console.groq.com) (for AI extraction)
            3. Enter keys above and test connection
            4. Keys can also be stored in .env file as TAVILY_API_KEY and GROQ_API_KEY
            """)
    
    # Show current status
    if data is None or data.empty:
        st.warning("⚠️ No data found. Please upload and filter your CSV data first.")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("← Go to Upload", width="stretch"):
                try:
                    from controllers import go_to_stage
                    go_to_stage('upload')
                except:
                    if 'current_stage' in st.session_state:
                        st.session_state.current_stage = 'upload'
                    st.rerun()

        with col2:
            # Create sample data for testing
            if st.button("🧪 Use Sample Data", width="stretch"):
                sample_data = pd.DataFrame({
                    'Consignee Name': [
                        'Acme Timber Corporation',
                        'Global Wood Solutions',
                        'Teakwood Trading Inc',
                        'Premium Lumber LLC',
                        'Forest Products Co'
                    ],
                    'Product': ['Teak Wood', 'Plywood', 'Timber Logs', 'Lumber', 'Wood Panels'],
                    'Quantity': [100, 200, 150, 300, 75],
                    'Value': [10000, 25000, 18000, 45000, 8500],
                    'Consignee City': ['Mumbai', 'Delhi', 'Chennai', 'Bangalore', 'Kolkata']
                })
                
                st.session_state.working_data = sample_data
                st.success("✅ Sample timber business data loaded! Refresh page to continue.")
                st.rerun()
        
        return
    
    # Data overview
    st.subheader("📊 Data Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Find company column with proper error handling
    company_column = None
    for col in ['Consignee Name', 'Company Name', 'Company', 'Consignee', 'Business Name']:
        if col in data.columns:
            # Check if column has any string values
            try:
                non_null_values = data[col].dropna()
                if len(non_null_values) > 0:
                    company_column = col
                    break
            except Exception:
                continue
    
    if not company_column:
        # Use first string column
        for col in data.columns:
            if data[col].dtype == 'object':
                company_column = col
                break
    
    # Find city column for location context with proper error handling
    city_column = None
    for col in ['Consignee City', 'City', 'Location', 'Place']:
        if col in data.columns:
            try:
                non_null_values = data[col].dropna()
                if len(non_null_values) > 0:
                    city_column = col
                    break
            except Exception:
                continue
    
    with col1:
        st.metric("Total Records", len(data))
    
    with col2:
        unique_companies = data[company_column].nunique() if company_column else 0
        st.metric("Unique Companies", unique_companies)
    
    with col3:
        # Check if Research_Status column exists to show accurate metrics with safe handling
        research_status_col = None
        for col in ['Research_Status', 'research_status', 'Status']:
            if col in data.columns:
                try:
                    # Test if column is accessible and contains some data
                    test_values = data[col].dropna()
                    if len(test_values) > 0 or len(data[col]) > 0:  # Accept empty columns too
                        research_status_col = col
                        break
                except Exception:
                    continue
        
        if research_status_col:
            researched_count = len(data[data[research_status_col].isin(['found', 'not_found', 'Found', 'Not_found'])][company_column].dropna().unique())
        else:
            researched_count = len(st.session_state.research_results)
        st.metric("Researched", researched_count)
    
    with col4:
        if research_status_col:
            pending_count = len(data[data[research_status_col].isin(['pending', 'Pending']) | data[research_status_col].isna()][company_column].dropna().unique())
        else:
            pending_count = unique_companies - researched_count if unique_companies > researched_count else 0
        st.metric("Pending", pending_count)
    
    # Show data preview
    with st.expander("📋 Data Preview", expanded=False):
        st.dataframe(data.head(10), width="stretch")
    
    if not company_column:
        st.error("❌ Could not identify company name column. Please ensure your data has a column like 'Company Name' or 'Consignee Name'.")
        return
    
    # Research Configuration
    st.subheader("⚙️ Research Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        batch_size = st.slider("Batch Size", 1, 10, 3, help="Number of companies to research at once (smaller batches for API limits)")
        search_delay = st.slider("Search Delay (seconds)", 1.0, 5.0, 2.0, help="Delay between searches to avoid rate limiting")
    
    with col2:
        enable_government_search = st.checkbox("Enable Government Sources", value=True, help="Search government databases for business registration")
        enable_industry_search = st.checkbox("Enable Industry Sources", value=True, help="Search timber industry directories")
    
    # Show search strategy
    search_layers = ["General Business Search"]
    if enable_government_search:
        search_layers.append("Government Database Search")
    if enable_industry_search:
        search_layers.append("Timber Industry Directory Search")
    
    st.info(f"🎯 **Search Strategy**: {' + '.join(search_layers)}")
    
    # SMART NAVIGATION - Detect companies ready for email outreach
    if st.session_state.research_results or (research_status_col and len(data[data[research_status_col].isin(['found', 'Found'])]) > 0):
        st.markdown("---")
        st.subheader("🎯 Smart Navigation - Email Outreach Ready")
        
        # Check for companies with research results that have emails
        companies_ready_for_email = 0
        companies_with_emails = []
        
        # Check session research results first
        if st.session_state.research_results:
            for company, result in st.session_state.research_results.items():
                if result['status'] == 'found' and result.get('contacts'):
                    for contact in result.get('contacts', []):
                        if contact.get('email') and '@' in contact['email']:
                            companies_ready_for_email += 1
                            companies_with_emails.append({
                                'company': company,
                                'email': contact['email'],
                                'source': 'session_results'
                            })
                            break  # Only count once per company
        
        # Also check data with research status columns for found companies with emails
        if research_status_col:
            try:
                found_companies_data = data[data[research_status_col].isin(['found', 'Found'])]
                
                for email_col in [col for col in data.columns if 'email' in col.lower()]:
                    try:
                        # Safe string handling to avoid "Can only use .str accessor with string values!" error
                        email_series = found_companies_data[email_col].copy()
                        
                        # Convert to string and handle NaN/None values safely
                        email_series_str = email_series.fillna('').astype(str)
                        
                        found_with_emails = found_companies_data[
                            (email_series.notna()) &
                            (email_series != '') &
                            (email_series != 'Not found') &
                            (email_series_str.str.contains('@', na=False, regex=False))
                        ]

                        for _, row in found_with_emails.iterrows():
                            try:
                                company_name = row[company_column] if company_column else str(row.iloc[0])
                                email_addr = row[email_col]

                                # Check email status - only include if email not sent yet
                                email_status_ready = True  # Default to ready if no status column

                                # Look for email status columns
                                email_status_columns = ['email_status', 'Email_Status', 'Email Status', 'email_sent_status', 'Email_Sent_Status']
                                for status_col in email_status_columns:
                                    if status_col in data.columns:
                                        try:
                                            status_value = str(row[status_col]).lower().strip()
                                            # Only NOT ready if status explicitly shows email was sent
                                            if status_value in ['sent', 'delivered', 'bounced', 'failed', 'completed']:
                                                email_status_ready = False
                                                break
                                            # Ready if status is "not sent", "pending", empty, or nan
                                        except:
                                            pass  # If error reading status, assume ready

                                # Only add if email status is ready and avoid duplicates
                                if email_status_ready and not any(item['company'] == company_name and item['source'] == 'session_results' for item in companies_with_emails):
                                    companies_ready_for_email += 1
                                    companies_with_emails.append({
                                        'company': company_name,
                                        'email': email_addr,
                                        'source': f'data_column_{email_col}'
                                    })
                            except Exception as e:
                                st.warning(f"Error processing row for {email_col}: {e}")
                                continue
                        
                        break  # Use first email column found
                        
                    except Exception as e:
                        st.warning(f"Error processing email column {email_col}: {e}")
                        continue
                        
            except Exception as e:
                st.warning(f"Error checking email data from research status: {e}")
        
        # Check if there are any found companies (even without emails yet)
        found_companies_count = 0
        if research_status_col:
            try:
                found_companies_count = len(data[data[research_status_col].isin(['found', 'Found'])])
            except:
                pass

        # Show smart navigation if companies are ready OR if there are found companies
        if companies_ready_for_email > 0:
            col1, col2 = st.columns([2, 1])

            with col1:
                st.success(f"✅ **{companies_ready_for_email} companies** are researched and ready for email outreach!")

                # Show preview of ready companies
                if len(companies_with_emails) <= 3:
                    for item in companies_with_emails:
                        st.write(f"• **{item['company']}** → {item['email']}")
                else:
                    for item in companies_with_emails[:3]:
                        st.write(f"• **{item['company']}** → {item['email']}")
                    st.write(f"... and {companies_ready_for_email - 3} more companies")

            with col2:
                st.markdown("#### 🚀 Quick Action")
                if st.button("📧 Go to Email Outreach →",
                           type="primary",
                           width="stretch",
                           help=f"Jump directly to email outreach for {companies_ready_for_email} ready companies"):

                    # Ensure enhanced data is available for email outreach
                    if 'enhanced_data' not in st.session_state and st.session_state.research_results:
                        try:
                            from services.web_scraper import ResearchResultsManager
                            enhanced_data = ResearchResultsManager.merge_with_original_data(data, st.session_state.research_results)
                            st.session_state.enhanced_data = enhanced_data
                            st.session_state.working_data = enhanced_data
                        except Exception as e:
                            st.warning(f"Could not prepare enhanced data: {e}")

                    # CRITICAL FIX: Clear old recipients table to force rebuild with new data
                    if 'recipients_dataframe' in st.session_state:
                        del st.session_state.recipients_dataframe

                    # Navigate to email outreach
                    try:
                        from controllers import go_to_stage
                        go_to_stage('analyze')  # Email outreach is in analyze stage
                    except:
                        st.session_state.current_stage = 'analyze'
                        st.rerun()
        elif found_companies_count > 0:
            # Show button even if no emails yet, but companies are found
            col1, col2 = st.columns([2, 1])

            with col1:
                st.warning(f"⚠️ **{found_companies_count} companies** found but need email research to be ready for outreach.")
                st.write("Complete email research to enable direct email outreach.")

            with col2:
                st.markdown("#### 🚀 Quick Action")
                if st.button("📧 Go to Email Outreach →",
                           type="secondary",
                           width="stretch",
                           help=f"Jump to email outreach to complete research for {found_companies_count} found companies"):

                    # Ensure enhanced data is available for email outreach
                    if 'enhanced_data' not in st.session_state and st.session_state.research_results:
                        try:
                            from services.web_scraper import ResearchResultsManager
                            enhanced_data = ResearchResultsManager.merge_with_original_data(data, st.session_state.research_results)
                            st.session_state.enhanced_data = enhanced_data
                            st.session_state.working_data = enhanced_data
                        except Exception as e:
                            st.warning(f"Could not prepare enhanced data: {e}")

                    # CRITICAL FIX: Clear old recipients table to force rebuild with new data
                    if 'recipients_dataframe' in st.session_state:
                        del st.session_state.recipients_dataframe

                    # Navigate to email outreach
                    try:
                        from controllers import go_to_stage
                        go_to_stage('analyze')  # Email outreach is in analyze stage
                    except:
                        st.session_state.current_stage = 'analyze'
                        st.rerun()
        else:
            st.info("ℹ️ **Smart Navigation**: No companies ready for email outreach yet. Complete research to enable quick navigation to email campaigns.")
    
    # Research Execution - Smart Research Status Management
    st.subheader("🚀 AI Research Execution")
    
    if company_column:
        # Check if Research_Status column exists in data
        research_status_col = None
        for col in ['Research_Status', 'research_status', 'Status']:
            if col in data.columns:
                research_status_col = col
                break
        
        # Determine companies that need research based on Research_Status
        if research_status_col:
            # Only research companies with 'pending' status
            pending_mask = data[research_status_col].isin(['pending', 'Pending', None, '']) | data[research_status_col].isna()
            pending_companies_data = data[pending_mask]
            pending_companies = pending_companies_data[company_column].dropna().unique().tolist()
            
            # Companies already researched (found or not_found)
            already_researched_mask = data[research_status_col].isin(['found', 'not_found', 'Found', 'Not_found'])
            already_researched = data[already_researched_mask]
            researched_count = len(already_researched[company_column].dropna().unique())
            
            # Show research status breakdown
            found_count = len(data[data[research_status_col].isin(['found', 'Found'])][company_column].dropna().unique())
            not_found_count = len(data[data[research_status_col].isin(['not_found', 'Not_found'])][company_column].dropna().unique())
            
            st.success(f"✅ **Smart Research Management Active**")
            st.info(f"📊 **Status**: {found_count} Found | {not_found_count} Not Found | {len(pending_companies)} Pending | ⏭️ Skipping {researched_count} already researched")
        else:
            # Fallback to old logic if no Research_Status column
            all_companies = data[company_column].dropna().unique().tolist()
            researched_companies = set(st.session_state.research_results.keys())
            pending_companies = [c for c in all_companies if c not in researched_companies]
            st.warning("⚠️ No Research_Status column found. Using session-based tracking.")
            st.info("💡 **Tip**: Upload CSV with Research_Status column for smart research management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"📋 **Companies to research**: {len(pending_companies)}")
            
            if pending_companies:
                st.write("**Sample pending companies:**")
                for company in pending_companies[:3]:
                    # Show city if available
                    if city_column:
                        city_info = data[data[company_column] == company][city_column].iloc[0] if len(data[data[company_column] == company]) > 0 else ""
                        st.write(f"• {company}" + (f" ({city_info})" if city_info else ""))
                    else:
                        st.write(f"• {company}")
                if len(pending_companies) > 3:
                    st.write(f"... and {len(pending_companies) - 3} more")
            else:
                st.write("**No pending research needed** ✅")
        
        with col2:
            st.info(f"⚙️ **Configuration**:")
            st.write(f"• Batch size: {batch_size}")
            st.write(f"• Search delay: {search_delay}s")
            st.write(f"• Government search: {'✅' if enable_government_search else '❌'}")
            st.write(f"• Industry search: {'✅' if enable_industry_search else '❌'}")
            st.write(f"• Model: llama-3.3-70b-versatile")
        
        # Cost estimation
        estimated_cost = batch_size * 0.05  # Rough estimate
        st.warning(f"💰 **Estimated API Cost**: ~${estimated_cost:.2f} for {batch_size} businesses")
        
        # Research buttons
        if pending_companies and st.session_state.api_tested:
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                if st.button("🔍 Start AI Research", type="primary", width="stretch"):
                    enhanced_batch_research(
                        pending_companies[:batch_size], 
                        search_delay, 
                        city_column, 
                        data, 
                        company_column,
                        enable_government_search,
                        enable_industry_search
                    )
            
            with col2:
                if st.button("🔄 Reset Results", width="stretch"):
                    st.session_state.research_results = {}
                    st.session_state.research_status = 'ready'
                    st.success("🔄 Results reset!")
                    st.rerun()
            
            with col3:
                if st.button("⏸️ Stop Research", disabled=True, width="stretch"):
                    st.info("Stop functionality coming soon")
        
        elif not st.session_state.api_tested:
            st.warning("⚠️ Please test API connection first before starting research")
        
        elif not pending_companies:
            st.success("✅ All companies have been researched!")
            
            # Show already researched companies for visibility
            if research_status_col:
                with st.expander("📊 View Already Researched Companies", expanded=False):
                    found_companies = data[data[research_status_col].isin(['found', 'Found'])][company_column].dropna().unique()
                    not_found_companies = data[data[research_status_col].isin(['not_found', 'Not_found'])][company_column].dropna().unique()
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"🟢 **Found ({len(found_companies)}):**")
                        for company in found_companies[:5]:
                            st.write(f"• {company}")
                        if len(found_companies) > 5:
                            st.write(f"... and {len(found_companies) - 5} more")
                    
                    with col2:
                        st.write(f"🔴 **Not Found ({len(not_found_companies)}):**")
                        for company in not_found_companies[:5]:
                            st.write(f"• {company}")
                        if len(not_found_companies) > 5:
                            st.write(f"... and {len(not_found_companies) - 5} more")
    
    # Results Section
    if st.session_state.research_results:
        st.subheader("📋 Research Results")
        
        # Results overview
        total = len(st.session_state.research_results)
        successful = len([r for r in st.session_state.research_results.values() if r['status'] == 'found'])
        with_descriptions = len([r for r in st.session_state.research_results.values() if r.get('description') and r['description'] != 'No description'])
        success_rate = (successful / total * 100) if total > 0 else 0.0
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Researched", total)
        with col2:
            st.metric("Successful", successful)
        with col3:
            st.metric("Success Rate", f"{success_rate:.1f}%")
        with col4:
            st.metric("With Descriptions", with_descriptions)
        
        # Results table with enhanced display
        with st.expander("👁️ View Research Results", expanded=True):
            try:
                from services.web_scraper import ResearchResultsManager
                results_df = ResearchResultsManager.format_results_for_display(st.session_state.research_results)
                st.dataframe(results_df, width="stretch")
            except Exception as e:
                # Fallback display
                results_data = []
                for company, result in st.session_state.research_results.items():
                    if result['status'] == 'found':
                        contacts = result.get('contacts', [])
                        primary_email = contacts[0]['email'] if contacts else 'No email'
                        phone = contacts[0].get('phone', 'No phone') if contacts else 'No phone'
                        website = result.get('website', 'No website')
                        description = result.get('description', 'No description')
                        confidence = f"{result.get('confidence_score', 0):.0%}"
                    else:
                        primary_email = phone = website = confidence = 'Failed'
                        description = result.get('description', 'Research failed')
                    
                    results_data.append({
                        'Company': company,
                        'Status': result['status'].title(),
                        'Email': primary_email,
                        'Phone': phone,
                        'Website': website,
                        'Description': description,
                        'Confidence': confidence
                    })
                
                results_df = pd.DataFrame(results_data)
                st.dataframe(results_df, width="stretch")
        
        # Export options
        st.subheader("📤 Export Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Export research results
            if 'results_df' in locals():
                results_csv = results_df.to_csv(index=False)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label="📥 Download Research Results",
                    data=results_csv,
                    file_name=f"timber_research_results_{timestamp}.csv",
                    mime="text/csv",
                    width="stretch"
                )
        
        with col2:
            # Export enhanced original data with PERSISTENCE - FIXED TO USE ORIGINAL FULL DATA
            try:
                from services.web_scraper import ResearchResultsManager
                
                # CRITICAL FIX: Use original full dataset, not filtered data
                original_full_data = None
                
                # Priority 1: Get original unfiltered data from state management
                try:
                    from state_management import get_state
                    state = get_state()
                    if hasattr(state, 'original_dataframe') and state.original_dataframe is not None:
                        original_full_data = state.original_dataframe
                        st.caption("✅ Using original full dataset from state management")
                    elif hasattr(state, 'main_dataframe') and state.main_dataframe is not None:
                        original_full_data = state.main_dataframe
                        st.caption("✅ Using main dataset from state management")
                except Exception as e:
                    st.caption(f"⚠️ Could not load from state management: {e}")
                
                # Priority 2: Check session state for original data
                if original_full_data is None:
                    if 'original_dataframe' in st.session_state and st.session_state.original_dataframe is not None:
                        original_full_data = st.session_state.original_dataframe
                        st.caption("✅ Using original dataset from session state")
                    elif 'main_dataframe' in st.session_state and st.session_state.main_dataframe is not None:
                        original_full_data = st.session_state.main_dataframe
                        st.caption("✅ Using main dataset from session state")
                
                # Priority 3: Check enhanced data as backup
                if original_full_data is None and 'enhanced_data' in st.session_state and st.session_state.enhanced_data is not None:
                    original_full_data = st.session_state.enhanced_data
                    st.caption("🔄 Using enhanced data as fallback")
                
                # Final fallback to current data
                if original_full_data is None:
                    original_full_data = data
                    st.caption("⚠️ Using current filtered data as fallback")
                
                # Merge research results with ORIGINAL FULL DATA
                enhanced_data = ResearchResultsManager.merge_with_original_data(original_full_data, st.session_state.research_results)
                
                # CRITICAL: Save enhanced data back to session state for persistence
                st.session_state.enhanced_data = enhanced_data
                st.session_state.working_data = enhanced_data  # Also update working_data

                # IMPORTANT: If we were using filtered data, update it with research results
                if st.session_state.get('research_uses_filtered_data', False):
                    st.session_state.filtered_data_for_research = enhanced_data

                # Also update state management for persistence across pages
                try:
                    from state_management import get_state
                    state = get_state()
                    state.working_data = enhanced_data
                    if hasattr(state, 'enhanced_data'):
                        state.enhanced_data = enhanced_data
                except Exception as e:
                    st.caption(f"Note: Could not update state management: {e}")
                
                enhanced_csv = enhanced_data.to_csv(index=False)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Show data size info to user
                st.info(f"📊 **Download Info**: {len(enhanced_data):,} total records (complete dataset + research results)")
                
                downloaded = st.download_button(
                    label="📊 Download Research Data",
                    data=enhanced_csv,
                    file_name=f"enhanced_timber_data_{timestamp}.csv",
                    mime="text/csv",
                    width="stretch",
                    key="download_enhanced_data"
                )
                
                if downloaded:
                    st.success("✅ **Complete dataset downloaded successfully!**")
                    st.info("🔄 Contains all original data + research results")
                
            except Exception as e:
                st.button("📊 Download Research Data", disabled=True, help=f"Error: {e}")
    
    # Navigation
    st.subheader("🧭 Navigation")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("← Back to Upload", use_container_width=True):
            try:
                from controllers import go_to_stage
                go_to_stage('upload')
            except:
                st.session_state.current_stage = 'upload'
                st.rerun()
    
    with col2:
        if st.button("🔄 Refresh Page", use_container_width=True):
            st.rerun()
    
    with col3:
        if st.session_state.research_results:
            if st.button("Email Outreach →", type="primary", use_container_width=True):
                # CRITICAL FIX: Clear old recipients table to force rebuild with new data
                if 'recipients_dataframe' in st.session_state:
                    del st.session_state.recipients_dataframe

                try:
                    from controllers import go_to_stage
                    go_to_stage('analyze')
                except:
                    st.session_state.current_stage = 'analyze'
                    st.rerun()
        else:
            st.button("Complete Research First", disabled=True, use_container_width=True)


def enhanced_batch_research(companies: List[str], delay: float, city_column: str, data: pd.DataFrame, 
                          company_column: str, enable_gov: bool, enable_industry: bool):
    """Enhanced batch research with real API calls"""
    
    progress_bar = st.progress(0.0, text="Initializing enhanced research...")
    status_text = st.empty()
    results_container = st.empty()
    
    try:
        from services.web_scraper import WebScraper
        scraper = WebScraper()
        
        # Check if scraper is properly initialized
        if scraper.researcher is None:
            st.error("❌ Enhanced researcher not properly initialized. Check API keys.")
            return
        
        for i, company in enumerate(companies):
            # Update progress
            progress = (i + 1) / len(companies)
            progress_bar.progress(progress, text=f"Researching {company}...")
            status_text.info(f"🔍 Enhanced research: {company}")
            
            # Get expected city if available
            expected_city = None
            if city_column:
                try:
                    city_data = data[data[company_column] == company][city_column]
                    expected_city = city_data.iloc[0] if len(city_data) > 0 else None
                except:
                    pass
            
            # Perform research
            try:
                result = scraper.research_company_contacts(company, expected_city)
                st.session_state.research_results[company] = result
                
                # Show live results
                if result['status'] == 'found':
                    contacts = result.get('contacts', [])
                    email = contacts[0]['email'] if contacts else 'No email'
                    description = result.get('description', 'No description')
                    status_text.success(f"✅ Found: {company} | Email: {email}")
                    
                    # Show description if available
                    if description and description != 'No description':
                        with results_container.container():
                            st.info(f"📝 **{company}**: {description[:150]}...")
                else:
                    status_text.warning(f"⚠️ Limited data: {company}")
                
            except Exception as e:
                st.session_state.research_results[company] = scraper.create_fallback_result(company, str(e))
                status_text.error(f"❌ Error: {company} - {str(e)[:50]}")
            
            # Delay between requests
            time.sleep(delay)
    
    except ImportError:
        st.error("❌ Enhanced web scraper not available. Please check installation.")
        return
    except Exception as e:
        st.error(f"❌ Research error: {e}")
        return
    
    # Final status and AUTOMATIC DATA PERSISTENCE
    successful = len([r for r in st.session_state.research_results.values() if r['status'] == 'found'])
    total = len(companies)
    
    # Auto-save enhanced data to session state for persistence - FIXED TO USE ORIGINAL FULL DATA
    try:
        from services.web_scraper import ResearchResultsManager
        
        # CRITICAL FIX: Use original full dataset for auto-save, not filtered data
        original_full_data = None
        
        # Get original unfiltered data from state management or session state
        try:
            from state_management import get_state
            state = get_state()
            if hasattr(state, 'original_dataframe') and state.original_dataframe is not None:
                original_full_data = state.original_dataframe
            elif hasattr(state, 'main_dataframe') and state.main_dataframe is not None:
                original_full_data = state.main_dataframe
        except Exception:
            pass
        
        # Check session state for original data
        if original_full_data is None:
            if 'original_dataframe' in st.session_state and st.session_state.original_dataframe is not None:
                original_full_data = st.session_state.original_dataframe
            elif 'main_dataframe' in st.session_state and st.session_state.main_dataframe is not None:
                original_full_data = st.session_state.main_dataframe
            elif 'enhanced_data' in st.session_state and st.session_state.enhanced_data is not None:
                original_full_data = st.session_state.enhanced_data
            else:
                original_full_data = data  # Fallback to current data
        
        # Merge with ORIGINAL FULL DATA
        enhanced_data = ResearchResultsManager.merge_with_original_data(original_full_data, st.session_state.research_results)
        
        # CRITICAL: Auto-save enhanced data back to session state
        st.session_state.enhanced_data = enhanced_data
        st.session_state.working_data = enhanced_data

        # IMPORTANT: If we were using filtered data, update it with research results
        if st.session_state.get('research_uses_filtered_data', False):
            st.session_state.filtered_data_for_research = enhanced_data
            st.info("🔄 **Updated filtered dataset with research results**")

        # Also update state management for persistence across pages
        try:
            from state_management import get_state
            state = get_state()
            state.working_data = enhanced_data
            if hasattr(state, 'enhanced_data'):
                state.enhanced_data = enhanced_data
        except Exception as e:
            st.caption(f"Note: Could not update state management: {e}")

        progress_bar.progress(1.0, text="Research completed - Data saved!")
        status_text.success(f"🎉 AI research completed! {successful}/{total} successful | 💾 Complete dataset saved ({len(enhanced_data):,} total records)")
        st.info("✅ Research results merged with complete dataset and saved to all data sources")
        
    except Exception as e:
        progress_bar.progress(1.0, text="Research completed!")
        status_text.success(f"🎉 AI research completed! {successful}/{total} successful")
        st.warning(f"⚠️ Auto-save warning: {str(e)}")
    
    st.session_state.research_status = 'completed'
    time.sleep(2)
    st.rerun()


# Alternative entry points
def render():
    """Entry point for existing app structure"""
    enhanced_business_research_page()


def main():
    """Main entry point"""
    enhanced_business_research_page()


if __name__ == "__main__":
    st.set_page_config(
        page_title="Enhanced Business Research",
        page_icon="🔍",
        layout="wide"
    )
    main()
