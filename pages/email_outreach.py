"""
Email Outreach Page - CRITICAL DATA FLOW FIX
=============================================
CRITICAL FIXES APPLIED:
1. 🔄 **DATA FLOW CONSISTENCY FIXED**: Recipients table now preserves CSV data properly
   - Added _get_csv_data_with_email_status() function to prioritize CSV data
   - Fixed get_unified_data_source() to check CSV data first
   - Enhanced _has_meaningful_email_status() to detect actual email status vs defaults
   - Recipients table initialization now preserves existing email status from CSV

2. 📧 **CSV EMAIL STATUS PRESERVATION**: Email status from input CSV is maintained
   - Recipients table no longer re-initializes and overwrites CSV data
   - Email status, sent dates, and campaign names preserved from uploaded file
   - Smart detection of meaningful email status vs empty defaults
   - Proper data sync between CSV input and Recipients Table display

3. 💾 **SESSION STATE MANAGEMENT**: Enhanced CSV data preservation
   - Added preserve_csv_data_on_upload() for session persistence
   - Original CSV data stored in session state for continuity
   - Email status continuity maintained across page refreshes
   - Enhanced merge logic for existing recipients data

4. 🔀 **UNIFIED DATA PRIORITY**: CSV data gets highest priority in data source selection
   - Modified priority order: CSV with email status → Enhanced data → Working data → Display → Main
   - Email status preservation takes precedence over other data transformations
   - Consistent data flow from upload → recipients table → merge back

ALL PREVIOUS FEATURES PRESERVED:
- Smart email column detection and mapping
- Enhanced campaign validation and error handling  
- Real-time recipient count updates
- Actual email sending with progress tracking
- Improved user feedback and debug info
"""

import streamlit as st
import pandas as pd
import asyncio
import io
from datetime import datetime
from state_management import get_state
from controllers import go_to_stage, get_display_dataframe
from utils.layout import render_header, render_file_info
from services.business_emailer import BusinessEmailer, get_email_provider_config
import logging

# Configure logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def render():
    """Render the Email Outreach page with ENHANCED functionality."""
    render_header("📧 Email Outreach - ENHANCED", "With Real Email Sending & Data Flow Fix")
    
    state = get_state()
    
    # Show file info
    render_file_info()
    
    # Check if we have data
    if state.main_dataframe is None:
        st.warning("⚠️ No data loaded. Please upload a file first.")
        if st.button("Go to Upload"):
            go_to_stage("upload")
        return
    
    # Get the best available data source with FIXED priority (CSV first)
    df = get_unified_data_source_FIXED(state)
    
    if df is None or len(df) == 0:
        st.warning("⚠️ No data to analyze. Please check your filters in the upload page.")
        if st.button("Go to Upload"):
            go_to_stage("upload")
        return
    
    # Show data source information
    show_data_source_info(df)
    
    # Initialize emailer
    if 'emailer' not in st.session_state:
        st.session_state.emailer = BusinessEmailer()
    
    emailer = st.session_state.emailer
    
    # STEP 1: Email Configuration
    st.markdown("---")
    st.header("🔧 Step 1: Email Configuration")
    render_email_configuration_simple(emailer)
    
    # STEP 2: Template Setup  
    st.markdown("---")
    st.header("📝 Step 2: Email Template Setup")
    template_ready = render_template_setup_simple(emailer)
    
    # STEP 3: Recipients Management (FIXED VERSION)
    st.markdown("---")  
    st.header("👥 Step 3: Recipients Selection & Management - FIXED")
    recipients_ready = render_recipients_FIXED(df, emailer)
    
    # STEP 4: Send Campaign (ENHANCED WITH BUTTON FIX)
    st.markdown("---")
    st.header("🚀 Step 4: Send Email Campaign")
    if recipients_ready:
        render_campaign_enhanced_FIXED(emailer, template_ready)
    else:
        st.info("⚠️ Please select recipients in Step 3 before sending campaigns")


# ============================================================================
# CRITICAL DATA FLOW FIX FUNCTIONS
# ============================================================================

def _get_csv_data_with_email_status(state):
    """
    CRITICAL: Get original CSV data if it has email status columns
    
    This is the key fix - prioritize the uploaded CSV file if it contains email status
    """
    try:
        logger.info("🔍 Checking for CSV data with email status...")
        
        # Check if we have original CSV data preserved somewhere
        if hasattr(state, 'original_dataframe') and state.original_dataframe is not None:
            original_df = state.original_dataframe.copy()
            
            # Check if this original data has meaningful email status
            if _has_meaningful_email_status(original_df):
                logger.info("Found original CSV with meaningful email status")
                return original_df
        
        # Check main_dataframe if it came from CSV with email status
        if state.main_dataframe is not None:
            main_df = state.main_dataframe.copy()
            
            if _has_meaningful_email_status(main_df):
                logger.info("Found main dataframe with meaningful email status")
                return main_df
        
        # Check session state for preserved CSV data
        if 'original_csv_data' in st.session_state and st.session_state.original_csv_data is not None:
            csv_df = st.session_state.original_csv_data.copy()
            
            if _has_meaningful_email_status(csv_df):
                logger.info("Found session CSV data with meaningful email status")
                return csv_df
        
        logger.info("No CSV data with meaningful email status found")
        return None
        
    except Exception as e:
        logger.warning(f"Error getting CSV data with email status: {str(e)}")
        return None


def _has_meaningful_email_status(df):
    """
    Check if DataFrame has meaningful email status data (not just default values)
    
    Args:
        df: DataFrame to check
        
    Returns:
        bool: True if meaningful email status exists
    """
    try:
        if df is None or df.empty:
            return False
        
        email_status_columns = ['email_selected', 'email_status', 'sent_date', 'campaign_name']
        
        # Check if columns exist
        existing_cols = [col for col in email_status_columns if col in df.columns]
        if not existing_cols:
            return False
        
        # Check for meaningful (non-default) values
        for col in existing_cols:
            if col == 'email_selected':
                # Any True values indicate meaningful data
                if df[col].any():
                    return True
            elif col == 'email_status':
                # Any non-default status values
                meaningful_statuses = df[col][(df[col] != 'Not Sent') & (df[col] != '') & df[col].notna()]
                if len(meaningful_statuses) > 0:
                    return True
            elif col in ['sent_date', 'campaign_name']:
                # Any non-empty values
                meaningful_values = df[col][(df[col] != '') & df[col].notna()]
                if len(meaningful_values) > 0:
                    return True
        
        return False
        
    except Exception as e:
        logger.warning(f"Error checking meaningful email status: {str(e)}")
        return False


def get_unified_data_source_FIXED(state):
    """
    CRITICAL FIX: Get data source with proper priority for research results

    Priority Order (FIXED):
    1. Enhanced data with research results - HIGHEST PRIORITY (contains latest research)
    2. CSV data with email status (from upload)
    3. Working data with email status
    4. Display dataframe
    5. Main dataframe
    """
    logger.info("🔍 Starting unified data source selection with research results priority...")

    # PRIORITY 1: Check for enhanced data with research results FIRST
    if 'enhanced_data' in st.session_state and st.session_state.enhanced_data is not None:
        enhanced_df = st.session_state.enhanced_data.copy()
        # Check if this enhanced data contains research results
        if st.session_state.get('research_results', {}):
            st.success("✅ **Using Enhanced Data with Research Results (Most up-to-date)**")
            logger.info("Selected enhanced data with research results as primary source")
            # Ensure email status columns exist
            if not has_email_status_in_dataframe(enhanced_df):
                enhanced_df = add_default_email_status_columns(enhanced_df)
            # Apply recipient changes if they exist
            if 'recipients_dataframe' in st.session_state:
                enhanced_df = sync_recipients_to_primary_data(enhanced_df, st.session_state.recipients_dataframe)
            return enhanced_df

    # PRIORITY 2: Check for original CSV data with email status
    csv_data_source = _get_csv_data_with_email_status(state)
    if csv_data_source is not None:
        st.info("🔄 **Using Original CSV Data (Email status preserved from file)**")
        logger.info("Selected CSV data as primary source")
        return csv_data_source
    
    # PRIORITY 3+: Fallback to other data sources with email status priority
    data_sources = []

    # Check working_data
    if hasattr(state, 'working_data') and state.working_data is not None:
        working_df = state.working_data.copy()
        has_email_status = has_email_status_in_dataframe(working_df)
        data_sources.append(('working_data', working_df, has_email_status, "Working data from state"))
    
    # Check display dataframe
    display_df = get_display_dataframe()
    if display_df is not None:
        display_df = display_df.copy()
        has_email_status = has_email_status_in_dataframe(display_df)
        data_sources.append(('display_data', display_df, has_email_status, "Display dataframe (filtered or main)"))
    
    # Check main dataframe
    if state.main_dataframe is not None:
        main_df = state.main_dataframe.copy()
        has_email_status = has_email_status_in_dataframe(main_df)
        data_sources.append(('main_dataframe', main_df, has_email_status, "Main dataframe"))
    
    if not data_sources:
        logger.warning("No data sources available")
        return None
    
    # Sort by email status priority (True first)
    data_sources.sort(key=lambda x: x[2], reverse=True)
    source_name, df, has_email_status, description = data_sources[0]
    
    if has_email_status:
        st.success(f"✅ **Using {description} (Email status preserved)**")
    else:
        st.info(f"🔄 **Using {description} (Adding default email status)**")
        df = add_default_email_status_columns(df)
    
    # SYNC FIX: Always apply recipient changes if they exist
    if 'recipients_dataframe' in st.session_state:
        df = sync_recipients_to_primary_data(df, st.session_state.recipients_dataframe)
    
    logger.info(f"Selected {source_name} as data source (has_email_status: {has_email_status})")
    return df


def prepare_recipients_dataframe_FIXED(businesses_df, email_col):
    """
    FIXED: Initialize recipients table without overwriting existing email status
    
    Key Fix: Preserve existing email status data from CSV input
    """
    logger.info("🔄 Initializing recipients table with CSV data preservation...")
    
    # Create working dataframe
    work_df = pd.DataFrame()
    
    # Smart business name detection (existing logic)
    business_name_value = get_business_name_column_smart(businesses_df)
    work_df['business_name'] = business_name_value
    work_df['email_address'] = businesses_df[email_col]
    
    # CRITICAL FIX: Preserve existing email status instead of resetting
    if has_email_status_in_dataframe(businesses_df):
        logger.info("📧 Preserving existing email status from CSV")
        
        # Preserve existing email status columns
        work_df['selected_for_email'] = businesses_df.get('email_selected', False)
        work_df['email_status'] = businesses_df.get('email_status', 'Not Sent')
        work_df['sent_date'] = businesses_df.get('sent_date', '')
        work_df['campaign_name'] = businesses_df.get('campaign_name', '')
        
        st.info("📧 **Email status preserved from input CSV file**")
        
    else:
        logger.info("📧 Adding default email status columns")
        
        # Add default columns for new data
        work_df['selected_for_email'] = False
        work_df['email_status'] = 'Not Sent'
        work_df['sent_date'] = ''
        work_df['campaign_name'] = ''
    
    # Store original indices for mapping
    work_df['original_index'] = businesses_df.index
    work_df['source_email_column'] = email_col
    
    # Add optional columns
    optional_columns = ['Phone_Number', 'Phone', 'Website', 'website', 'Research_Status', 'description']
    for col in optional_columns:
        if col in businesses_df.columns:
            if col == 'Phone_Number':
                work_df['Phone'] = businesses_df[col]
            elif col == 'Phone':
                work_df['Phone'] = businesses_df[col]
            elif col in ['Website', 'website']:
                work_df['Website'] = businesses_df[col]
    
    work_df = work_df.reset_index(drop=True)
    
    logger.info(f"Recipients table initialized with {len(work_df)} records")
    return work_df


def merge_existing_recipients_data_FIXED(existing_df, new_df):
    """
    FIXED: Merge existing recipients data with new data properly preserving email status
    """
    try:
        logger.info("🔄 Merging recipients data...")
        
        if existing_df.empty or 'original_index' not in existing_df.columns:
            logger.info("No existing data to merge, using new data")
            return new_df
        
        # Create mapping of original indices to existing email status
        existing_mapping = {}
        for idx, row in existing_df.iterrows():
            original_idx = row.get('original_index')
            if pd.notna(original_idx):
                existing_mapping[original_idx] = {
                    'selected_for_email': row.get('selected_for_email', False),
                    'email_status': row.get('email_status', 'Not Sent'),
                    'sent_date': row.get('sent_date', ''),
                    'campaign_name': row.get('campaign_name', ''),
                    'email_address': row.get('email_address', '')
                }
        
        # Update new_df with preserved existing data
        merged_count = 0
        for idx, row in new_df.iterrows():
            original_idx = row.get('original_index')
            if pd.notna(original_idx) and original_idx in existing_mapping:
                existing_data = existing_mapping[original_idx]
                
                # Preserve all email status fields
                new_df.loc[idx, 'selected_for_email'] = existing_data['selected_for_email']
                new_df.loc[idx, 'email_status'] = existing_data['email_status']
                new_df.loc[idx, 'sent_date'] = existing_data['sent_date']
                new_df.loc[idx, 'campaign_name'] = existing_data['campaign_name']
                
                # Preserve email address changes if they exist
                if existing_data['email_address'] and existing_data['email_address'] != row.get('email_address', ''):
                    new_df.loc[idx, 'email_address'] = existing_data['email_address']
                
                merged_count += 1
        
        if merged_count > 0:
            st.success(f"✅ **Merged {merged_count} records with existing email status**")
            logger.info(f"Successfully merged {merged_count} records")
        else:
            st.info("🔄 No existing email status to merge")
        
        return new_df
        
    except Exception as e:
        st.warning(f"⚠️ Could not merge existing data: {str(e)}")
        logger.warning(f"Merge error: {str(e)}")
        return new_df


def preserve_csv_data_on_upload(df):
    """
    CRITICAL: Preserve original CSV data in session state for email status continuity
    """
    try:
        # Store original CSV data in session state
        st.session_state.original_csv_data = df.copy()
        
        # Check if CSV has email status
        if _has_meaningful_email_status(df):
            st.session_state.csv_has_email_status = True
            st.success("📧 **CSV with email status preserved in session**")
            logger.info("CSV data with email status preserved in session")
        else:
            st.session_state.csv_has_email_status = False
            logger.info("CSV data without email status preserved in session")
        
    except Exception as e:
        logger.error(f"Error preserving CSV data: {str(e)}")


# ============================================================================
# FIXED RECIPIENTS MANAGEMENT FUNCTION
# ============================================================================

def select_best_email_column(df):
    """
    Smart email column selection - prioritizes columns with actual email data
    """
    # Find all email columns
    email_columns = [col for col in df.columns if 'email' in col.lower()]
    if not email_columns:
        return None, []

    # Priority ranking for email columns (higher score = higher priority)
    priority_patterns = {
        'primary_email': 100,
        'email_address': 90,
        'email': 80,
        'business_email': 70,
        'contact_email': 60,
        'company_email': 50,
        # Lower priority for status/selection columns
        'email_status': 10,
        'email_selected': 5,
        'email_sent': 5
    }

    # Score each column
    column_scores = []
    for col in email_columns:
        col_lower = col.lower()

        # Base priority score
        priority_score = 0
        for pattern, score in priority_patterns.items():
            if pattern in col_lower:
                priority_score = max(priority_score, score)

        # If no specific pattern matched, give default score
        if priority_score == 0:
            priority_score = 30

        # Check for actual email data (contains '@')
        try:
            email_data_count = df[col].astype(str).str.contains('@', na=False).sum()
            data_score = email_data_count * 10  # Bonus for actual email data
        except:
            data_score = 0

        total_score = priority_score + data_score
        column_scores.append((col, total_score, email_data_count))

    # Sort by total score (highest first)
    column_scores.sort(key=lambda x: x[1], reverse=True)

    # Return best column and all columns info
    best_column = column_scores[0][0]
    return best_column, email_columns


def render_recipients_FIXED(df, emailer):
    """FIXED Recipients Management - With CSV Email Status Preservation"""

    # Smart email column selection
    email_col, email_columns = select_best_email_column(df)
    if not email_col:
        st.error("❌ No email columns found in data")
        return False

    st.info(f"🔍 Using email column: **{email_col}**")
    
    # DEBUG: Show sample email data before validation (collapsible)
    with st.expander("🔍 **Email Validation Debug Info** (Click to expand if there are issues)", expanded=False):
        st.write("**DEBUG: Email Column Analysis:**")
        st.write(f"- Email column: {email_col}")
        st.write(f"- Total rows: {len(df)}")
        st.write(f"- Non-null values: {df[email_col].notna().sum()}")
        st.write(f"- Non-empty values: {(df[email_col] != '').sum()}")
        st.write(f"- Sample values: {df[email_col].dropna().head(5).tolist()}")
        
        # More detailed validation with debugging
        try:
            # Step 1: Check not null
            step1_mask = df[email_col].notna()
            step1_count = step1_mask.sum()
            st.write(f"- Step 1 (not null): {step1_count} records")
            
            # Step 2: Check not empty 
            step2_mask = step1_mask & (df[email_col] != '')
            step2_count = step2_mask.sum()
            st.write(f"- Step 2 (not empty): {step2_count} records")
            
            # Step 3: Check not 'Not found'
            step3_mask = step2_mask & (df[email_col] != 'Not found')
            step3_count = step3_mask.sum()
            st.write(f"- Step 3 (not 'Not found'): {step3_count} records")
            
            # Step 4: Check contains '@'
            # Convert to string first to avoid errors
            df_str_email = df[email_col].astype(str)
            step4_mask = step3_mask & df_str_email.str.contains('@', na=False)
            step4_count = step4_mask.sum()
            st.write(f"- Step 4 (contains '@'): {step4_count} records")
            
            # Show some failing examples if validation fails
            if step4_count == 0 and len(df) > 0:
                st.warning("**DEBUG: No valid emails found. Sample failing values:**")
                failing_emails = df[~step4_mask][email_col].head(10).tolist()
                for i, email in enumerate(failing_emails):
                    st.write(f"  {i+1}. '{email}' (type: {type(email)})")
            
            # Use the final mask for filtering
            valid_email_mask = step4_mask
            
        except Exception as e:
            st.error(f"**DEBUG: Error in email validation:** {str(e)}")
            # Fallback to simpler validation
            valid_email_mask = (
                df[email_col].notna() & 
                (df[email_col].astype(str) != '') & 
                (df[email_col].astype(str) != 'Not found') &
                df[email_col].astype(str).str.contains('@', na=False)
            )
    
    # QUICK FIX: Try more lenient validation first
    try:
        # Simple and robust email validation
        valid_email_mask = (
            df[email_col].notna() & 
            (df[email_col].astype(str).str.strip() != '') & 
            (df[email_col].astype(str) != 'Not found') &
            (df[email_col].astype(str) != 'nan') &
            df[email_col].astype(str).str.contains('@', na=False, regex=False)
        )
    except Exception as e:
        st.error(f"**Error in email validation:** {str(e)}")
        # Ultra-simple fallback
        valid_email_mask = df[email_col].astype(str).str.contains('@', na=False)
    
    businesses_with_emails = df[valid_email_mask].copy()
    
    if businesses_with_emails.empty:
        st.error("❌ No businesses with valid email addresses found")
        # Show additional debug info
        st.write("**DEBUG: All email validation steps failed. Check the data above.")
        
        # EMERGENCY FALLBACK: Try different email columns if available
        st.warning("🔄 **ATTEMPTING EMERGENCY RECOVERY**")
        for alt_email_col in email_columns[1:]:  # Try other email columns
            st.write(f"Trying alternative email column: {alt_email_col}")
            try:
                alt_mask = (
                    df[alt_email_col].notna() & 
                    (df[alt_email_col].astype(str) != '') & 
                    df[alt_email_col].astype(str).str.contains('@', na=False)
                )
                alt_count = alt_mask.sum()
                st.write(f"- {alt_email_col}: {alt_count} valid emails")
                if alt_count > 0:
                    st.success(f"✅ **Recovery successful!** Using {alt_email_col} instead.")
                    email_col = alt_email_col  # Switch to working column
                    valid_email_mask = alt_mask
                    businesses_with_emails = df[valid_email_mask].copy()
                    break
            except Exception as alt_e:
                st.write(f"- {alt_email_col}: Failed ({str(alt_e)})")
        
        # If still no valid emails found
        if businesses_with_emails.empty:
            st.error("❌ **RECOVERY FAILED:** No valid emails found in any email column")
            
            # Show user data for manual inspection
            with st.expander("🔍 **Manual Data Inspection** (Click to expand)"):
                st.write("**All Email Columns and Sample Data:**")
                for col in email_columns:
                    st.write(f"**{col}:**")
                    sample_data = df[col].dropna().head(10)
                    if len(sample_data) > 0:
                        for i, val in enumerate(sample_data):
                            st.write(f"  {i+1}. '{val}' (type: {type(val)})")
                    else:
                        st.write("  No data in this column")
                    st.write("")
            
            # Offer manual override option
            st.warning("⚙️ **Manual Override Options:**")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🔧 Skip Email Validation (UNSAFE)", 
                           help="Proceed without valid emails - for debugging only"):
                    st.warning("⚠️ Proceeding without email validation!")
                    businesses_with_emails = df.copy()  # Use all data
                    # Add dummy email column if needed
                    if email_col not in businesses_with_emails.columns:
                        businesses_with_emails[email_col] = "no-email@example.com"
            
            with col2:
                if st.button("📤 Export Data for Review", 
                           help="Download data to check email formats manually"):
                    # Export current dataframe for manual review
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_data = csv_buffer.getvalue()
                    
                    st.download_button(
                        label="📥 Download for Manual Review",
                        data=csv_data,
                        file_name=f"email_validation_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                    
            if businesses_with_emails.empty:
                return False
    
    st.success(f"📧 Found {len(businesses_with_emails)} businesses with email addresses")
    
    # ENHANCED: Show column mapping information
    detected_business_col = detect_business_name_column_info(businesses_with_emails)
    if detected_business_col:
        st.info(f"🏢 Using business name column: **{detected_business_col}**")
    
    # Create a working dataframe with FIXED initialization (preserves CSV email status)
    work_df = prepare_recipients_dataframe_FIXED(businesses_with_emails, email_col)
    
    # FIXED: Initialize or update session state with proper merging
    if 'recipients_dataframe' not in st.session_state:
        st.session_state.recipients_dataframe = work_df.copy()
        st.info("🔄 Initialized recipients table from CSV data")
    else:
        # FIXED: Merge existing session data with current data properly
        existing_df = st.session_state.recipients_dataframe
        updated_df = merge_existing_recipients_data_FIXED(existing_df, work_df)
        st.session_state.recipients_dataframe = updated_df
        st.success("🔄 Recipients table updated with preserved email status")
    
    # Get the current working dataframe - ensure it reflects latest changes
    current_df = st.session_state.recipients_dataframe.copy()

    # Force refresh if changes were just saved
    if 'force_table_refresh' in st.session_state and st.session_state.force_table_refresh:
        st.session_state.force_table_refresh = False
        current_df = st.session_state.recipients_dataframe.copy()
    
    # Show summary stats
    total_count = len(current_df)
    selected_count = current_df['selected_for_email'].sum()
    sent_count = (current_df['email_status'] == 'Sent').sum()
    available_count = total_count - sent_count
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📊 Total", total_count)
    col2.metric("✅ Selected", int(selected_count))
    col3.metric("📤 Sent", sent_count) 
    col4.metric("📨 Available", available_count)
    
    # Show email status preservation info
    if sent_count > 0 or selected_count > 0:
        st.success(f"✅ **Email status preserved:** {sent_count} sent, {int(selected_count)} selected")
    
    # Quick Selection - Enhanced with Auto-merge
    st.markdown("#### 🎯 Quick Selection")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("✅ Select All Available", width="stretch"):
            mask = st.session_state.recipients_dataframe['email_status'] != 'Sent'
            st.session_state.recipients_dataframe.loc[mask, 'selected_for_email'] = True
            st.rerun()

    with col2:
        if st.button("❌ Deselect All", width="stretch"):
            st.session_state.recipients_dataframe['selected_for_email'] = False
            st.rerun()

    with col3:
        if st.button("📥 Download Complete Data", width="stretch", help="Download complete recipients data with email status"):
            # Use the latest recipients dataframe from session state to ensure saved changes are included
            latest_recipients_df = st.session_state.recipients_dataframe
            st.info("🔄 **Using latest saved recipients data** for download (includes all edits)")
            download_complete_recipients_data_FIXED(latest_recipients_df)
    
    # STREAMLINED RECIPIENTS TABLE - Simplified editing interface
    st.markdown("#### 📋 Recipients Table - Streamlined Editing")

    # Add editing mode toggle for better user experience
    col1, col2 = st.columns([3, 1])
    with col1:
        edit_mode = st.toggle("✏️ Edit Mode", value=True, help="Toggle to enable/disable editing")
        if edit_mode:
            st.success("✏️ **Edit Mode Active** - Click in table cells to make changes, then save.")
        else:
            st.info("👁️ **View Mode** - Enable Edit Mode to make changes.")
    with col2:
        # Show data freshness indicator
        if 'data_updated_timestamp' in st.session_state:
            last_update = st.session_state.data_updated_timestamp
            st.caption(f"🕒 Last saved: {last_update[-8:-3]}")  # Show time only

    # Create display dataframe with proper column names
    display_df = current_df.copy()
    display_df = display_df.rename(columns={
        'selected_for_email': '✅ Select',
        'business_name': 'Business Name',
        'email_address': 'Email Address',
        'email_status': 'Status',
        'sent_date': 'Sent Date',
        'campaign_name': 'Campaign'
    })

    # Show only relevant columns for display
    display_columns = ['✅ Select', 'Business Name', 'Email Address', 'Status']
    if 'Sent Date' in display_df.columns and not display_df['Sent Date'].isna().all():
        display_columns.append('Sent Date')
    if 'Campaign' in display_df.columns and not display_df['Campaign'].isna().all():
        display_columns.append('Campaign')

    # Filter to show only available columns
    available_display_cols = [col for col in display_columns if col in display_df.columns]
    table_df = display_df[available_display_cols]

    # STREAMLINED DATA EDITOR - Simplified configuration
    if edit_mode:
        edited_df = st.data_editor(
            table_df,
            width="stretch",
            num_rows="fixed",
            column_config={
                '✅ Select': st.column_config.CheckboxColumn(
                    '✅ Select',
                    help="Select for email campaign",
                    default=False
                ),
                'Business Name': st.column_config.TextColumn(
                    'Business Name',
                    help="Company name",
                    max_chars=100,
                    disabled=True
                ),
                'Email Address': st.column_config.TextColumn(
                    'Email Address',
                    help="Email address - editable",
                    validate=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                ),
                'Status': st.column_config.SelectboxColumn(
                    'Status',
                    help="Email status",
                    options=['Not Sent', 'Sent', 'Failed', 'Bounced'],
                    default='Not Sent'
                )
            },
            key="recipients_editor_streamlined"
        )

        # STREAMLINED SAVE - Single button with clear feedback
        col1, col2, col3 = st.columns([2, 1, 1])
        with col2:
            if st.button("💾 Save Changes", type="primary", width="stretch"):
                with st.spinner("Saving changes..."):
                    save_recipients_changes_streamlined(edited_df)
                    # Force update the current_df to reflect changes immediately
                    st.session_state.force_table_refresh = True
                st.success("✅ Changes saved successfully!")
                st.rerun()  # Refresh to show updated data
        with col3:
            if st.button("🔄 Reset", width="stretch", help="Reset to original data"):
                st.rerun()
    else:
        # Read-only view when edit mode is disabled - use current_df to show saved changes
        readonly_df = current_df.copy()
        readonly_df = readonly_df.rename(columns={
            'selected_for_email': '✅ Select',
            'business_name': 'Business Name',
            'email_address': 'Email Address',
            'email_status': 'Status',
            'sent_date': 'Sent Date',
            'campaign_name': 'Campaign'
        })

        # Show same columns as edit mode
        readonly_columns = ['✅ Select', 'Business Name', 'Email Address', 'Status']
        if 'Sent Date' in readonly_df.columns and not readonly_df['Sent Date'].isna().all():
            readonly_columns.append('Sent Date')
        if 'Campaign' in readonly_df.columns and not readonly_df['Campaign'].isna().all():
            readonly_columns.append('Campaign')

        available_readonly_cols = [col for col in readonly_columns if col in readonly_df.columns]
        readonly_table_df = readonly_df[available_readonly_cols]

        st.dataframe(
            readonly_table_df,
            width="stretch",
            column_config={
                '✅ Select': st.column_config.CheckboxColumn('✅ Select'),
                'Business Name': st.column_config.TextColumn('Business Name'),
                'Email Address': st.column_config.TextColumn('Email Address'),
                'Status': st.column_config.TextColumn('Status')
            }
        )
        st.info("ℹ️ Enable 'Edit Mode' above to make changes to the table.")
    
    # Show current selection for campaign
    selected_for_campaign = st.session_state.recipients_dataframe[
        st.session_state.recipients_dataframe['selected_for_email'] == True
    ]
    
    if len(selected_for_campaign) > 0:
        st.success(f"🎯 **{len(selected_for_campaign)} recipients selected** for email campaign")
        return True
    else:
        st.warning("⚠️ No recipients selected. Use the selection buttons above.")
        return False


# ============================================================================
# EXISTING FUNCTIONS (PRESERVED)
# ============================================================================

def has_email_status_in_dataframe(df):
    """Check if dataframe has email status columns."""
    if df is None or df.empty:
        return False
    
    email_status_columns = ['email_selected', 'email_status', 'sent_date', 'campaign_name']
    return any(col in df.columns for col in email_status_columns)


def add_default_email_status_columns(df):
    """Add default email status columns to dataframe - FIXED: Ensures consistent column ordering."""
    if df is None:
        return df
    
    df = df.copy()
    
    # CRITICAL FIX: Store original columns and ensure email status columns are always added at the END
    original_columns = df.columns.tolist()
    email_status_columns = ['email_selected', 'email_status', 'sent_date', 'campaign_name']
    
    # Remove email status columns if they already exist (to re-add them at the end)
    columns_to_keep = [col for col in original_columns if col not in email_status_columns]
    df_reordered = df[columns_to_keep].copy()
    
    # Add email status columns at the END in consistent order
    if 'email_selected' not in df_reordered.columns:
        df_reordered['email_selected'] = df.get('email_selected', False)
    if 'email_status' not in df_reordered.columns:
        df_reordered['email_status'] = df.get('email_status', 'Not Sent')
    if 'sent_date' not in df_reordered.columns:
        df_reordered['sent_date'] = df.get('sent_date', '')
    if 'campaign_name' not in df_reordered.columns:
        df_reordered['campaign_name'] = df.get('campaign_name', '')
    
    return df_reordered


# DEPRECATED: Old complex sync function replaced with simplified version
# The new sync_recipients_to_primary_data() function provides more reliable data persistence


def show_data_source_info(df):
    """Show information about the current data source"""
    if df is not None:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("📊 Total Rows", len(df))
        col2.metric("📋 Total Columns", len(df.columns))
        
        # Check if email management columns exist
        email_cols = [col for col in df.columns if col in ['email_selected', 'email_status', 'sent_date', 'campaign_name']]
        col3.metric("📧 Email Mgmt", len(email_cols))
        
        # Show email columns found
        email_columns = [col for col in df.columns if 'email' in col.lower()]
        col4.metric("📧 Email Columns", len(email_columns))
        
        if email_columns:
            st.info(f"📧 Email columns: {', '.join(email_columns[:3])}" + (f" (+{len(email_columns)-3} more)" if len(email_columns) > 3 else ""))


def detect_business_name_column_info(businesses_df):
    """Helper function to detect and return the name of the business column being used"""
    
    # Priority 1: Check if 'business_name' column exists
    if 'business_name' in businesses_df.columns:
        return 'business_name'
    
    # Priority 2: Look for common business name column patterns
    business_name_patterns = [
        'Consignee Name',  # Import/Export data
        'Company Name', 
        'Business Name',
        'Organization Name',
        'Firm Name',
        'Entity Name',
        'Client Name',
        'Customer Name'
    ]
    
    for pattern in business_name_patterns:
        if pattern in businesses_df.columns:
            return pattern
    
    # Priority 3: Look for columns containing 'name' (case insensitive)
    name_columns = [col for col in businesses_df.columns if 'name' in col.lower()]
    for col in name_columns:
        # Skip columns that are likely not business names
        skip_patterns = ['file', 'user', 'contact', 'person', 'individual']
        if not any(skip in col.lower() for skip in skip_patterns):
            return col
    
    # Priority 4: Check first few columns and avoid obvious non-business columns
    exclude_patterns = ['date', 'time', 'id', 'index', 'code', 'number', 'qty', 'quantity', 'price', 'rate', 'value', 'amount']
    
    for i, col in enumerate(businesses_df.columns[:5]):  # Check first 5 columns
        col_lower = col.lower()
        # Skip columns that are obviously not business names
        if not any(pattern in col_lower for pattern in exclude_patterns):
            # Additional check: ensure the column contains text data, not dates or numbers
            sample_values = businesses_df[col].dropna().head(3)
            if not sample_values.empty:
                # Check if values look like business names (contain letters, not just dates/numbers)
                sample_str = str(sample_values.iloc[0])
                if (len(sample_str) > 3 and 
                    any(c.isalpha() for c in sample_str) and 
                    not sample_str.replace('-', '').replace('/', '').replace(' ', '').isdigit()):
                    return col
    
    # Fallback
    return businesses_df.columns[0] if len(businesses_df.columns) > 0 else None


def get_business_name_column_smart(businesses_df):
    """FIXED: Smart detection of business name column to avoid date columns"""
    
    # Priority 1: Check if 'business_name' column exists
    if 'business_name' in businesses_df.columns:
        return businesses_df['business_name']
    
    # Priority 2: Look for common business name column patterns
    business_name_patterns = [
        'Consignee Name',  # Import/Export data
        'Company Name', 
        'Business Name',
        'Organization Name',
        'Firm Name',
        'Entity Name',
        'Client Name',
        'Customer Name'
    ]
    
    for pattern in business_name_patterns:
        if pattern in businesses_df.columns:
            return businesses_df[pattern]
    
    # Priority 3: Look for columns containing 'name' (case insensitive)
    name_columns = [col for col in businesses_df.columns if 'name' in col.lower()]
    for col in name_columns:
        # Skip columns that are likely not business names
        skip_patterns = ['file', 'user', 'contact', 'person', 'individual']
        if not any(skip in col.lower() for skip in skip_patterns):
            return businesses_df[col]
    
    # Priority 4: Check first few columns and avoid obvious non-business columns
    exclude_patterns = ['date', 'time', 'id', 'index', 'code', 'number', 'qty', 'quantity', 'price', 'rate', 'value', 'amount']
    
    for i, col in enumerate(businesses_df.columns[:5]):  # Check first 5 columns
        col_lower = col.lower()
        # Skip columns that are obviously not business names
        if not any(pattern in col_lower for pattern in exclude_patterns):
            # Additional check: ensure the column contains text data, not dates or numbers
            sample_values = businesses_df[col].dropna().head(3)
            if not sample_values.empty:
                # Check if values look like business names (contain letters, not just dates/numbers)
                sample_str = str(sample_values.iloc[0])
                if (len(sample_str) > 3 and 
                    any(c.isalpha() for c in sample_str) and 
                    not sample_str.replace('-', '').replace('/', '').replace(' ', '').isdigit()):
                    return businesses_df[col]
    
    # Priority 5: Fallback to first column with warning
    st.warning(f"⚠️ Could not identify business name column. Using first column: '{businesses_df.columns[0]}'. Please verify data correctness.")
    return businesses_df.iloc[:, 0]


# ============================================================================
# EXISTING FUNCTIONS (UNCHANGED)
# ============================================================================

def render_email_configuration_simple(emailer):
    """Simple email configuration"""
    
    with st.expander("📧 Email Provider Setup", expanded=not emailer.is_configured):
        # Email method selection
        email_method = st.radio(
            "Choose Email Method",
            options=['smtp', 'resend', 'sendgrid', 'cloud_free'],
            format_func=lambda x: {
                'smtp': '📧 SMTP (Gmail/Outlook) - Works locally, may fail in cloud',
                'resend': '⚡ Resend (Modern) - Recommended for cloud production',
                'sendgrid': '🚀 SendGrid (Professional) - Alternative cloud option',
                'cloud_free': '🌐 Free Cloud Email Service - Basic fallback'
            }[x],
            help="Resend or SendGrid are recommended for reliable cloud email delivery. SMTP may be blocked in cloud deployments."
        )

        if email_method == 'smtp':
            col1, col2 = st.columns(2)

            with col1:
                provider = st.selectbox(
                    "Email Provider",
                    options=['gmail', 'outlook', 'yahoo'],
                    format_func=lambda x: {'gmail': '📧 Gmail', 'outlook': '📧 Outlook', 'yahoo': '📧 Yahoo'}[x]
                )

            with col2:
                provider_configs = {
                    'gmail': {'smtp_server': 'smtp.gmail.com', 'port': 587},
                    'outlook': {'smtp_server': 'smtp-mail.outlook.com', 'port': 587},
                    'yahoo': {'smtp_server': 'smtp.mail.yahoo.com', 'port': 587}
                }
                config = provider_configs[provider]
                st.info(f"SMTP: {config['smtp_server']}:{config['port']}")

        elif email_method == 'resend':
            st.info("⚡ **Resend Modern Email Service**")

            # Check if API key is set
            import os
            api_key = os.environ.get('RESEND_API_KEY')

            if not api_key:
                st.error("🔑 **Resend API Key Required**")

                with st.expander("📖 Resend Setup Guide (2 minutes)", expanded=True):
                    st.markdown("""
                    ### **Step 1: Create Resend Account**
                    1. Go to [Resend.com](https://resend.com)
                    2. Click "Get Started"
                    3. Sign up with GitHub/Google (very quick!)

                    ### **Step 2: Get API Key**
                    1. Login to Resend Dashboard
                    2. Go to "API Keys" section
                    3. Click "Create API Key"
                    4. Name: "TeakWood Business App"
                    5. Copy the API key (starts with `re_`)

                    ### **Step 3: Add to Railway**
                    1. Go to your Railway dashboard
                    2. Select your project → Variables tab
                    3. Add new variable:
                       - **Name**: `RESEND_API_KEY`
                       - **Value**: Your Resend API key
                    4. Save and restart your app

                    ### **Benefits:**
                    - ✅ **3,000 emails/month FREE** forever
                    - ✅ **Modern API** - Developer friendly
                    - ✅ **Works in all cloud deployments**
                    - ✅ **No SMTP port issues**
                    - ✅ **Fast setup** - No domain verification needed
                    - ✅ **Great deliverability**
                    """)

                st.warning("⚠️ Add `RESEND_API_KEY` to Railway environment variables to continue")
            else:
                st.success(f"✅ **Resend API Key Configured** (Key: {api_key[:8]}...)")
                st.info("💡 **Ready for modern email delivery** - 3,000 emails/month free tier")

            config = {'smtp_server': 'resend_api', 'port': 443}

        elif email_method == 'sendgrid':
            st.info("🚀 **SendGrid Professional Email Service**")

            # Check if API key is set
            import os
            api_key = os.environ.get('SENDGRID_API_KEY')

            if not api_key:
                st.error("🔑 **SendGrid API Key Required**")

                with st.expander("📖 SendGrid Setup Guide (5 minutes)", expanded=True):
                    st.markdown("""
                    ### **Step 1: Create SendGrid Account**
                    1. Go to [SendGrid.com](https://sendgrid.com)
                    2. Click "Start for Free"
                    3. Sign up and verify your email

                    ### **Step 2: Get API Key**
                    1. Login to SendGrid Dashboard
                    2. Go to Settings → API Keys
                    3. Click "Create API Key"
                    4. Name: "TeakWood Business App"
                    5. Permissions: "Full Access"
                    6. Copy the API key (starts with `SG.`)

                    ### **Step 3: Add to Railway**
                    1. Go to your Railway dashboard
                    2. Select your project → Variables tab
                    3. Add new variable:
                       - **Name**: `SENDGRID_API_KEY`
                       - **Value**: Your SendGrid API key
                    4. Save and restart your app

                    ### **Step 4: Verify Sender**
                    1. In SendGrid: Settings → Sender Authentication
                    2. Click "Verify a Single Sender"
                    3. Enter your email details and verify

                    ### **Benefits:**
                    - ✅ **100 emails/day free** forever
                    - ✅ **Works in all cloud deployments**
                    - ✅ **Professional deliverability**
                    - ✅ **No SMTP port issues**
                    - ✅ **Detailed analytics**
                    """)

                st.warning("⚠️ Add `SENDGRID_API_KEY` to Railway environment variables to continue")
            else:
                st.success(f"✅ **SendGrid API Key Configured** (Key: {api_key[:8]}...)")
                st.info("💡 **Ready for professional email delivery** - 100 emails/day free tier")

            config = {'smtp_server': 'sendgrid_api', 'port': 443}

        elif email_method == 'cloud_free':
            st.info("🌐 **Free Cloud Email Service (Web3Forms)**")

            # Check if API key is set
            import os
            api_key = os.environ.get('WEB3FORMS_ACCESS_KEY')

            if not api_key:
                st.warning("⚠️ **API Key Required** - Get your free Web3Forms access key:")

                with st.expander("📖 How to Get Free API Key (2 minutes setup)"):
                    st.markdown("""
                    **Step 1:** Go to [Web3Forms.com](https://web3forms.com)

                    **Step 2:** Click "Get Started Free"

                    **Step 3:** Enter your email address

                    **Step 4:** Copy your Access Key

                    **Step 5:** Add to Railway Environment Variables:
                    - Variable Name: `WEB3FORMS_ACCESS_KEY`
                    - Variable Value: Your access key from Web3Forms

                    **Benefits:**
                    - ✅ **Completely Free** - No credit card required
                    - ✅ **1000 emails/month** free tier
                    - ✅ **Works in all cloud deployments**
                    - ✅ **No SMTP port issues**
                    - ✅ **Better deliverability**
                    """)

                st.error("🔑 **Setup Required**: Add WEB3FORMS_ACCESS_KEY to your Railway environment variables")
            else:
                st.success(f"✅ **Web3Forms API Key Configured** (Key: {api_key[:8]}...)")

            config = {'smtp_server': 'cloud_api', 'port': 443}
        
        # Cloud deployment info
        import os
        is_cloud = any(env_var in os.environ for env_var in ['RAILWAY_ENVIRONMENT', 'RAILWAY_PROJECT_ID'])
        if is_cloud:
            st.warning("🌐 **Cloud Deployment Detected**: Many cloud platforms block SMTP ports for security. If emails fail to send, consider using a cloud email service like SendGrid, Mailgun, or AWS SES for production use.")
            with st.expander("📖 Cloud Email Setup Guide"):
                st.markdown("""
                **For Production Email in Cloud Deployments:**

                1. **SendGrid** (Recommended)
                   - Free tier: 100 emails/day
                   - Easy API integration
                   - Good deliverability

                2. **Mailgun**
                   - Free tier: 5,000 emails/month
                   - Reliable service
                   - Simple setup

                3. **AWS SES**
                   - Very low cost
                   - High volume capable
                   - Requires AWS account

                **Current Setup**: Using direct SMTP (may not work in all cloud environments)
                """)

        with st.form("email_config"):
            if email_method == 'smtp':
                email = st.text_input("📧 Email Address", placeholder="your.email@gmail.com")
                password = st.text_input("🔐 App Password", type="password", help="Use App Password for Gmail")
                sender_name = st.text_input("👤 Display Name", placeholder="Your Company Name")
            elif email_method == 'resend':
                email = st.text_input("📧 From Email Address", placeholder="your.email@company.com", help="Any email address works with Resend")
                password = "resend_api_token"  # Placeholder for Resend
                sender_name = st.text_input("👤 Display Name", placeholder="Your Company Name")

                import os
                if os.environ.get('RESEND_API_KEY'):
                    st.success("💡 **Resend API configured** - Modern service (3,000 emails/month free)")
                else:
                    st.error("💡 **Resend API key required** - Add RESEND_API_KEY to environment variables")
            elif email_method == 'sendgrid':
                email = st.text_input("📧 From Email Address", placeholder="your.email@company.com", help="Must be verified in SendGrid")
                password = "sendgrid_api_token"  # Placeholder for SendGrid
                sender_name = st.text_input("👤 Display Name", placeholder="Your Company Name")

                import os
                if os.environ.get('SENDGRID_API_KEY'):
                    st.success("💡 **SendGrid API configured** - Professional service (100 emails/day free)")
                else:
                    st.error("💡 **SendGrid API key required** - Add SENDGRID_API_KEY to environment variables")
            else:  # cloud_free
                email = st.text_input("📧 From Email Address", placeholder="your.email@company.com", help="This will appear as the sender")
                password = "cloud_service_token"  # Placeholder for cloud service
                sender_name = st.text_input("👤 Display Name", placeholder="Your Company Name")

                import os
                if os.environ.get('WEB3FORMS_ACCESS_KEY'):
                    st.success("💡 **Web3Forms API configured** - Premium free service (1000 emails/month)")
                else:
                    st.info("💡 **No API key needed** - Using FormSubmit.co free service (works immediately)")

            if st.form_submit_button("🔧 Configure Email", type="primary"):
                if email and password:
                    emailer.configure_smtp(
                        smtp_server=config['smtp_server'],
                        port=config['port'], 
                        email=email,
                        password=password,
                        sender_name=sender_name or "Business Team"
                    )
                    
                    success, message = emailer.test_email_config()
                    if success:
                        st.success(f"✅ {message}")
                    else:
                        st.error(f"❌ {message}")
                else:
                    st.error("❌ Please provide email and password")
    
    if emailer.is_configured:
        st.success(f"✅ Email configured: {emailer.sender_name} <{emailer.email}>")

        # Add test email functionality
        with st.expander("🧪 Test Email Service"):
            test_email = st.text_input("Test Email Address", placeholder="test@example.com")
            if st.button("📧 Send Test Email", type="secondary"):
                if test_email:
                    with st.spinner("Sending test email..."):
                        success, message = emailer.send_test_email(test_email)
                        if success:
                            st.success(f"✅ Test email sent successfully to {test_email}")
                        else:
                            st.error(f"❌ Test email failed: {message}")
                else:
                    st.error("Please enter a test email address")


def render_template_setup_simple(emailer):
    """Simple template setup"""
    
    with st.expander("📝 Email Template Setup", expanded=False):
        # Get available templates
        template_names = emailer.get_template_list()
        
        if not template_names:
            st.error("❌ No email templates available")
            return False
        
        selected_template = st.selectbox(
            "Choose Template",
            options=template_names,
            format_func=lambda x: {'business_intro': '🤝 Business Introduction', 'supply_inquiry': '🏭 Supply Inquiry'}.get(x, x)
        )
        
        # Simple template variables
        with st.form("template_setup"):
            st.markdown("**Template Variables:**")
            
            col1, col2 = st.columns(2)
            with col1:
                company_name = st.text_input("Your Company", value="Winwood Enterprise Sdn Bhd")
                sender = st.text_input("Your Name", value="Marketing Head : Mr Dominic") 
                phone = st.text_input("Phone", value="+60-17-8181903")
                
            with col2:
                email = st.text_input("Email", value="dominic@winwood.com.my")
                products = st.text_area("Product Requirements", value="Premium timber and wood products")
                volume = st.text_area("Volume Requirements", value="Bulk orders for construction and furniture")
            
            if st.form_submit_button("💾 Save Template Settings", type="primary"):
                template_vars = {
                    'your_company_name': company_name,
                    'sender_name': sender,
                    'your_phone': phone,
                    'your_email': email,
                    'product_requirements': products,
                    'volume_requirements': volume,
                    'timeline_requirements': 'Flexible delivery schedule',
                    'quality_requirements': 'Premium grade with certifications'
                }
                
                st.session_state.template_variables = template_vars
                st.session_state.selected_template = selected_template
                st.success("✅ Template configured successfully!")
    
    # Show current template status
    if 'template_variables' in st.session_state:
        template_name = st.session_state.get('selected_template', 'Unknown')
        st.success(f"✅ Template ready: {template_name}")
        return True
    else:
        st.warning("⚠️ Please configure template above")
        return False


def download_complete_recipients_data_FIXED(current_df):
    """DOWNLOAD FIX: Enable complete data download with email status properly synced"""

    try:
        # Get the unified data source to include all business information
        from state_management import get_state
        state = get_state()
        unified_df = get_unified_data_source_FIXED(state)

        if unified_df is not None and current_df is not None:

            # CRITICAL FIX: Use the streamlined sync function to ensure all changes are included
            st.info("🔄 Syncing Recipients Table data back to main dataframe...")

            # Use the same sync function that works for saving
            download_df = sync_recipients_to_primary_data(unified_df, current_df)

            # Ensure email management columns exist
            required_columns = ['email_selected', 'email_status', 'sent_date', 'campaign_name']
            for col in required_columns:
                if col not in download_df.columns:
                    if col == 'email_selected':
                        download_df[col] = False
                    elif col == 'email_status':
                        download_df[col] = 'Not Sent'
                    else:
                        download_df[col] = ''

            # Count synced records for user feedback
            sync_count = len(current_df)

            # Show sync results
            if sync_count > 0:
                st.success(f"✅ **Successfully synced {sync_count} recipients** back to main dataframe for download")
            else:
                st.warning("⚠️ No recipients data was synced back. Download will contain original data only.")

            # CRITICAL FIX: Also update all session state dataframes for consistency
            # Update the main data sources so other parts of the app also see the changes
            if sync_count > 0:
                # Update enhanced_data if it exists
                if 'enhanced_data' in st.session_state and st.session_state.enhanced_data is not None:
                    st.session_state.enhanced_data = download_df.copy()

                # Update working_data in state
                if hasattr(state, 'working_data'):
                    from state_management import update_state
                    update_state(working_data=download_df.copy())

                # Update main_dataframe in state
                if state.main_dataframe is not None:
                    from state_management import update_state
                    update_state(main_dataframe=download_df.copy())
                
                st.info("🔄 **All data sources updated** with Recipients Table changes")
            
            # Ensure email management columns exist with proper defaults
            email_mgmt_columns = ['email_selected', 'email_status', 'sent_date', 'campaign_name']
            for col in email_mgmt_columns:
                if col not in download_df.columns:
                    if col == 'email_selected':
                        download_df[col] = False
                    else:
                        download_df[col] = ''
            
            # Create CSV download
            csv_buffer = io.StringIO()
            download_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            # Generate download filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"complete_recipients_data_SYNCED_{timestamp}.csv"
            
            # Create download button
            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                width="stretch"
            )
            
            # Show download info with sync confirmation
            selected_count = (download_df['email_selected'] == True).sum() if 'email_selected' in download_df.columns else 0
            sent_count = (download_df['email_status'] == 'Sent').sum() if 'email_status' in download_df.columns else 0
            
            st.success(f"✅ Ready to download {len(download_df)} records with {len(download_df.columns)} columns")
            st.info(f"📧 **Email Status Summary:** {int(selected_count)} selected, {sent_count} sent")
            
            # Show column summary
            with st.expander("📋 Download Content Preview"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Email Management Columns:**")
                    email_cols = [col for col in download_df.columns if col in email_mgmt_columns]
                    for col in email_cols:
                        unique_vals = download_df[col].nunique()
                        if col == 'email_selected':
                            true_count = (download_df[col] == True).sum()
                            st.write(f"• {col}: {true_count} selected out of {len(download_df)}")
                        elif col == 'email_status':
                            status_counts = download_df[col].value_counts().to_dict()
                            st.write(f"• {col}: {status_counts}")
                        else:
                            non_empty = (download_df[col] != '').sum()
                            st.write(f"• {col}: {non_empty} non-empty values")
                
                with col2:
                    st.write("**Business Data Columns:**")
                    business_cols = [col for col in download_df.columns if col not in email_mgmt_columns][:5]
                    for col in business_cols:
                        st.write(f"• {col}")
                    if len(download_df.columns) - len(email_mgmt_columns) > 5:
                        st.write(f"• ... and {len(download_df.columns) - len(email_mgmt_columns) - 5} more columns")
                
                # Show preview of first few rows with email status highlighted
                st.write("**Data Preview (with Email Status):**")
                preview_cols = ['email_selected', 'email_status', 'sent_date'] + [col for col in download_df.columns if col not in ['email_selected', 'email_status', 'sent_date', 'campaign_name']][:3]
                preview_cols = [col for col in preview_cols if col in download_df.columns]
                st.dataframe(download_df[preview_cols].head(3))
        
        else:
            st.error("❌ No data available for download")
            
    except Exception as e:
        st.error(f"❌ Download failed: {str(e)}")
        import traceback
        st.error(f"Debug info: {traceback.format_exc()}")
        
        # Try to provide partial download as fallback
        try:
            if current_df is not None and len(current_df) > 0:
                st.warning("🔄 Attempting fallback download with Recipients Table data only...")
                
                csv_buffer = io.StringIO()
                current_df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"recipients_table_data_{timestamp}.csv"
                
                st.download_button(
                    label="📥 Download Recipients Data (Fallback)",
                    data=csv_data,
                    file_name=filename,
                    mime="text/csv",
                    width="stretch"
                )
                
                st.info("⚠️ Fallback download contains Recipients Table data only (may not include all business columns)")
        except Exception as fallback_error:
            st.error(f"❌ Fallback download also failed: {str(fallback_error)}")



def save_recipients_changes_streamlined(edited_df):
    """STREAMLINED: Simplified save function with reliable data persistence"""

    try:
        # Convert back to original column names
        save_df = edited_df.rename(columns={
            '✅ Select': 'selected_for_email',
            'Business Name': 'business_name',
            'Email Address': 'email_address',
            'Status': 'email_status',
            'Sent Date': 'sent_date',
            'Campaign': 'campaign_name'
        })

        # Ensure all required columns exist in save_df
        required_columns = ['selected_for_email', 'business_name', 'email_address', 'email_status', 'sent_date', 'campaign_name']
        for col in required_columns:
            if col not in save_df.columns:
                if col == 'selected_for_email':
                    save_df[col] = False
                elif col == 'email_status':
                    save_df[col] = 'Not Sent'
                else:
                    save_df[col] = ''

        # CRITICAL: Update session state recipients dataframe with all original columns preserved
        original_df = st.session_state.recipients_dataframe.copy()

        # Update only the editable columns while preserving all other data
        for idx in save_df.index:
            if idx < len(original_df):
                original_df.loc[idx, 'selected_for_email'] = save_df.loc[idx, 'selected_for_email']
                original_df.loc[idx, 'email_address'] = save_df.loc[idx, 'email_address']
                original_df.loc[idx, 'email_status'] = save_df.loc[idx, 'email_status']
                if 'sent_date' in save_df.columns:
                    original_df.loc[idx, 'sent_date'] = save_df.loc[idx, 'sent_date']
                if 'campaign_name' in save_df.columns:
                    original_df.loc[idx, 'campaign_name'] = save_df.loc[idx, 'campaign_name']

        # Update session state with the merged data
        st.session_state.recipients_dataframe = original_df

        # Track changes for user feedback
        selected_count = original_df['selected_for_email'].sum()

        # SIMPLIFIED: Focus on primary data source sync
        try:
            from state_management import get_state
            state = get_state()

            # Get the primary data source
            primary_df = None
            data_source = None
            if 'enhanced_data' in st.session_state and st.session_state.enhanced_data is not None:
                primary_df = st.session_state.enhanced_data
                data_source = "enhanced_data"
            elif hasattr(state, 'working_data') and state.working_data is not None:
                primary_df = state.working_data
                data_source = "working_data"
            elif state.main_dataframe is not None:
                primary_df = state.main_dataframe
                data_source = "main_dataframe"

            if primary_df is not None and data_source:
                # Sync changes back to primary data source
                updated_df = sync_recipients_to_primary_data(primary_df, original_df)

                # Update the primary data source
                if data_source == "enhanced_data":
                    st.session_state.enhanced_data = updated_df
                elif data_source == "working_data":
                    from state_management import update_state
                    update_state(working_data=updated_df)
                elif data_source == "main_dataframe":
                    from state_management import update_state
                    update_state(main_dataframe=updated_df)

                # Update timestamp for other components
                st.session_state.data_updated_timestamp = datetime.now().isoformat()

                st.success(f"✅ **Changes saved & synced!** {selected_count} recipients selected.")
                st.info(f"🔄 Data synchronized with {data_source}")
            else:
                st.success(f"✅ **Recipients table saved!** {selected_count} recipients selected.")
                st.warning("⚠️ No primary data source found for sync - changes saved locally.")

        except Exception as sync_error:
            st.success(f"✅ **Recipients table saved!** {selected_count} recipients selected.")
            st.warning(f"⚠️ Sync to main data failed: {str(sync_error)}")

    except Exception as e:
        st.error(f"❌ Save failed: {str(e)}")
        import traceback
        st.error(f"Debug info: {traceback.format_exc()}")


def sync_recipients_to_primary_data(primary_df, recipients_df):
    """SIMPLIFIED: Sync recipients table changes back to primary data source"""

    try:
        if primary_df is None or recipients_df is None or recipients_df.empty:
            return primary_df

        updated_df = primary_df.copy()

        # Find email column in primary dataframe - prioritize Primary_Email
        email_columns = [col for col in updated_df.columns if 'email' in col.lower()]
        if not email_columns:
            return updated_df

        # Prioritize Primary_Email column if it exists
        primary_email_col = None
        if 'Primary_Email' in updated_df.columns:
            primary_email_col = 'Primary_Email'
        elif any('primary' in col.lower() for col in email_columns):
            primary_email_col = next(col for col in email_columns if 'primary' in col.lower())
        else:
            primary_email_col = email_columns[0]

        # Add email management columns if they don't exist
        for col in ['email_selected', 'email_status', 'sent_date', 'campaign_name']:
            if col not in updated_df.columns:
                if col == 'email_selected':
                    updated_df[col] = False
                elif col == 'email_status':
                    updated_df[col] = 'Not Sent'
                else:
                    updated_df[col] = ''

        # Sync data using business name matching (more reliable than index)
        sync_count = 0

        for _, recipient_row in recipients_df.iterrows():
            business_name = recipient_row['business_name']

            # Find matching row in primary dataframe
            business_col = detect_business_name_column_info(updated_df)
            if business_col:
                mask = updated_df[business_col] == business_name
                matching_rows = updated_df[mask]

                if len(matching_rows) > 0:
                    idx = matching_rows.index[0]  # Use first match

                    # Update email and status information
                    updated_df.loc[idx, primary_email_col] = recipient_row['email_address']
                    updated_df.loc[idx, 'email_selected'] = recipient_row['selected_for_email']
                    updated_df.loc[idx, 'email_status'] = recipient_row['email_status']
                    updated_df.loc[idx, 'sent_date'] = recipient_row.get('sent_date', '')
                    updated_df.loc[idx, 'campaign_name'] = recipient_row.get('campaign_name', '')

                    sync_count += 1

        if sync_count > 0:
            st.success(f"✅ Synchronized {sync_count} records with primary data")

        return updated_df

    except Exception as e:
        st.warning(f"⚠️ Data sync error: {str(e)}")
        return primary_df


def render_campaign_enhanced_FIXED(emailer, template_ready):
    """ENHANCED campaign management with FIXED button enabling logic and REAL email sending"""
    
    if 'recipients_dataframe' not in st.session_state:
        st.error("❌ No recipients data available")
        return
    
    recipients_df = st.session_state.recipients_dataframe
    
    # 🔥 DUPLICATE PREVENTION FIX: Enhanced filtering to prevent duplicate emails
    available_recipients = recipients_df[
        (recipients_df['selected_for_email'] == True) &
        (recipients_df['email_status'] != 'Sent') &  # Exclude already sent
        (recipients_df['email_status'] != 'Sending') &  # Exclude currently sending
        (recipients_df['email_address'] != '') &  # Exclude empty emails
        (recipients_df['email_address'].notna())  # Exclude null emails
    ]
    
    # Additional duplicate check by email address
    if not available_recipients.empty:
        # Remove duplicate email addresses within selection
        available_recipients = available_recipients.drop_duplicates(subset=['email_address'], keep='first')
        
        # Check against previously sent emails in other campaigns
        previously_sent_emails = recipients_df[
            recipients_df['email_status'] == 'Sent'
        ]['email_address'].tolist()
        
        if previously_sent_emails:
            duplicate_mask = available_recipients['email_address'].isin(previously_sent_emails)
            if duplicate_mask.any():
                duplicate_count = duplicate_mask.sum()
                st.warning(f"⚠️ Filtered out {duplicate_count} recipients with emails already sent in previous campaigns")
                available_recipients = available_recipients[~duplicate_mask]
    
    if len(available_recipients) == 0:
        sent_count = (recipients_df['email_status'] == 'Sent').sum()
        if sent_count > 0:
            st.warning(f"⚠️ All {sent_count} selected recipients have already been sent emails. No new emails to send.")
        else:
            st.warning("⚠️ No recipients selected for campaign")
        return
    
    # ENHANCED: Real-time count display with sent status
    available_count = len(available_recipients)
    total_selected = (recipients_df['selected_for_email'] == True).sum()
    already_sent = total_selected - available_count
    
    if already_sent > 0:
        st.info(f"📊 **{total_selected} recipients selected** | ✅ {already_sent} already sent | 📧 **{available_count} ready to send**")
    else:
        st.success(f"🎯 Ready to send to **{available_count} selected recipients**")
    
    # ENHANCED: Step validation status
    col1, col2, col3 = st.columns(3)
    with col1:
        if emailer.is_configured:
            st.success("✅ Email Configured")
        else:
            st.error("❌ Email Not Configured")
    
    with col2:
        if template_ready:
            st.success("✅ Template Ready")  
        else:
            st.error("❌ Template Not Ready")
    
    with col3:
        if available_count > 0:
            st.success(f"✅ {available_count} Recipients Available")
        else:
            st.error("❌ No Recipients Available")
    
    # Campaign settings with NO FORM (to avoid Streamlit form button issues)
    st.markdown("#### ⚙️ Campaign Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        campaign_name = st.text_input(
            "Campaign Name", 
            value=f"Campaign_{datetime.now().strftime('%Y%m%d_%H%M')}",
            key="campaign_name_input"
        )
        
        # ENHANCED: Dynamic max emails based on available (not sent) selection
        max_emails = st.number_input(
            "Max Emails to Send",
            min_value=1,
            max_value=available_count,
            value=available_count,
            help=f"Maximum: {available_count} unsent recipients",
            key="max_emails_input"
        )
    
    with col2:
        delay_seconds = st.slider(
            "Delay Between Emails (seconds)",
            min_value=1.0, max_value=10.0, value=2.0, step=0.5,
            help="Delay to avoid overwhelming email servers",
            key="delay_slider"
        )
        
        # ENHANCED: More prominent confirmation
        confirm_send = st.checkbox(
            "✅ I confirm sending this campaign", 
            help="Required to enable the Send button",
            key="confirm_send_checkbox"
        )
    
    # ENHANCED: Dynamic warning message with actual available count
    emails_to_send = min(max_emails, available_count)
    st.warning(f"⚠️ Ready to send **{emails_to_send} emails** to unsent recipients only")
    
    # FIXED: Validation logic outside of form
    emailer_configured = bool(emailer.is_configured)
    template_is_ready = bool(template_ready)
    recipients_available = bool(available_count > 0)  # Changed from selected to available
    confirm_checked = bool(confirm_send)
    
    can_send = emailer_configured and template_is_ready and recipients_available and confirm_checked
    
    # Show real-time validation status
    st.markdown("**🔍 Current Status:**")
    debug_col1, debug_col2, debug_col3, debug_col4 = st.columns(4)
    with debug_col1:
        st.write(f"Email: {'✅' if emailer_configured else '❌'}")
    with debug_col2:
        st.write(f"Template: {'✅' if template_is_ready else '❌'}")
    with debug_col3:
        st.write(f"Available: {'✅' if recipients_available else '❌'}")
    with debug_col4:
        st.write(f"Confirm: {'✅' if confirm_checked else '❌'}")
    
    # Show what's missing if button should be disabled
    if not can_send:
        missing_items = []
        if not emailer_configured:
            missing_items.append("Configure Email (Step 1)")
        if not template_is_ready:
            missing_items.append("Setup Template (Step 2)")
        if not recipients_available:
            missing_items.append("Select Unsent Recipients (Step 3)")
        if not confirm_checked:
            missing_items.append("Check Confirmation Box")
        
        st.error(f"❌ **Missing Requirements:** {', '.join(missing_items)}")
    else:
        st.success("✅ **All requirements met!** Button is enabled.")
    
    # FIXED: Button outside of form to avoid form submission issues
    send_button = st.button(
        f"🚀 Send Campaign ({emails_to_send} emails)",
        type="primary",
        disabled=not can_send,
        help="All requirements must be met to enable this button" if not can_send else "Click to start sending emails",
        key="send_campaign_button",
        width="stretch"
    )
    
    # ENHANCED: Execute REAL EMAIL CAMPAIGN when button is clicked
    if send_button:
        if can_send:
            st.success("🚀 **Real Email Campaign Started!**")
            st.balloons()
            
            # Execute real email campaign with available recipients only
            execute_real_email_campaign(
                emailer=emailer,
                recipients_df=available_recipients.head(emails_to_send),  # Use available, not selected
                campaign_name=campaign_name,
                delay_seconds=delay_seconds
            )
            
        else:
            st.error("❌ **Button clicked but validation failed!** Please check requirements above.")


def update_main_data_pipeline(recipients_df):
    """INTEGRATION FIX: Update the main data pipeline with email campaign results"""
    
    try:
        state = get_state()
        
        # Update enhanced_data if it exists (priority 1)
        if 'enhanced_data' in st.session_state and st.session_state.enhanced_data is not None:
            updated_enhanced = sync_recipients_to_primary_data(
                st.session_state.enhanced_data,
                recipients_df
            )
            st.session_state.enhanced_data = updated_enhanced
            st.info("🔄 Enhanced data updated with email campaign results")

        # Update working_data if it exists (priority 2)
        if hasattr(state, 'working_data') and state.working_data is not None:
            updated_working = sync_recipients_to_primary_data(
                state.working_data,
                recipients_df
            )
            from state_management import update_state
            update_state(working_data=updated_working)
            st.info("📊 Working data updated with email campaign results")

        # Update main_dataframe as fallback
        if state.main_dataframe is not None:
            updated_main = sync_recipients_to_primary_data(
                state.main_dataframe,
                recipients_df
            )
            from state_management import update_state
            update_state(main_dataframe=updated_main)
            st.info("📋 Main dataframe updated with email campaign results")
        
        # INTEGRATION: Update session state to trigger other pages to refresh
        if 'data_updated_timestamp' not in st.session_state:
            st.session_state.data_updated_timestamp = datetime.now().isoformat()
        else:
            st.session_state.data_updated_timestamp = datetime.now().isoformat()
        
        st.success("✅ **UNIFIED DATA FLOW**: All data sources updated successfully!")
        
    except Exception as e:
        st.warning(f"⚠️ Data pipeline update warning: {str(e)}")


def execute_real_email_campaign(emailer, recipients_df, campaign_name, delay_seconds):
    """Execute real email campaign with progress tracking and error handling"""
    
    # Create progress tracking containers
    progress_bar = st.progress(0)
    status_container = st.empty()
    stats_container = st.container()
    
    # Campaign statistics
    total_recipients = len(recipients_df)
    sent_count = 0
    failed_count = 0
    
    # Get template information from session state
    template_name = st.session_state.get('selected_template', 'business_intro')
    template_vars = st.session_state.get('template_variables', {})
    
    # Show initial status
    status_container.info(f"🚀 Starting campaign: {campaign_name}")
    
    try:
        # Process each recipient
        for idx, (_, recipient) in enumerate(recipients_df.iterrows()):
            try:
                # Update progress
                progress = (idx + 1) / total_recipients
                progress_bar.progress(progress)
                status_container.info(f"📧 Sending to {recipient['business_name']}... ({idx + 1}/{total_recipients})")
                
                # DUPLICATE PREVENTION: Mark as 'Sending' to prevent concurrent sends
                original_idx = recipient.get('original_index')
                if original_idx is not None:
                    mask = st.session_state.recipients_dataframe['original_index'] == original_idx
                else:
                    mask = st.session_state.recipients_dataframe['business_name'] == recipient['business_name']
                
                st.session_state.recipients_dataframe.loc[mask, 'email_status'] = 'Sending'
                
                # CUSTOMIZATION: Prepare comprehensive business data for email template
                # Get original data from unified source for complete information
                state = get_state()
                unified_df = get_unified_data_source_FIXED(state)
                
                business_data = {
                    'business_name': recipient['business_name'],
                    'email': recipient['email_address']
                }
                
                # Extract additional business information from unified data if available
                if unified_df is not None and 'original_index' in recipient:
                    original_idx = recipient['original_index']
                    if original_idx in unified_df.index:
                        original_row = unified_df.loc[original_idx]
                        
                        # Add all available columns to business_data for template processing
                        for col in unified_df.columns:
                            if pd.notna(original_row[col]) and original_row[col] != '':
                                business_data[col.lower().replace(' ', '_')] = str(original_row[col])
                
                # Send actual email using the emailer service
                success, message = emailer.send_personalized_email(
                    recipient_email=recipient['email_address'],
                    business_data=business_data,
                    template_name=template_name,
                    variables=template_vars
                )
                
                if success:
                    sent_count += 1
                    
                    # Update recipient status in session state
                    original_idx = recipient.get('original_index')
                    if original_idx is not None:
                        mask = st.session_state.recipients_dataframe['original_index'] == original_idx
                    else:
                        mask = st.session_state.recipients_dataframe['business_name'] == recipient['business_name']
                    
                    st.session_state.recipients_dataframe.loc[mask, 'email_status'] = 'Sent'
                    st.session_state.recipients_dataframe.loc[mask, 'sent_date'] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    st.session_state.recipients_dataframe.loc[mask, 'campaign_name'] = campaign_name
                    
                    # Show success message
                    status_container.success(f"✅ Sent to {recipient['business_name']} ({idx + 1}/{total_recipients})")
                    
                else:
                    failed_count += 1
                    st.error(f"❌ Failed to send to {recipient['business_name']}: {message}")
                    
                    # Update recipient status as failed with error message
                    original_idx = recipient.get('original_index')
                    if original_idx is not None:
                        mask = st.session_state.recipients_dataframe['original_index'] == original_idx
                    else:
                        mask = st.session_state.recipients_dataframe['business_name'] == recipient['business_name']
                    
                    st.session_state.recipients_dataframe.loc[mask, 'email_status'] = 'Failed'
                    st.session_state.recipients_dataframe.loc[mask, 'sent_date'] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    st.session_state.recipients_dataframe.loc[mask, 'campaign_name'] = campaign_name
                    # Store the error message if there's a column for it
                    if 'error_message' in st.session_state.recipients_dataframe.columns:
                        st.session_state.recipients_dataframe.loc[mask, 'error_message'] = message
                
                # Show live statistics
                with stats_container:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("📧 Total", total_recipients)
                    col2.metric("✅ Sent", sent_count)
                    col3.metric("❌ Failed", failed_count)
                
                # Delay between emails (except for the last one)
                if idx < total_recipients - 1:
                    import time
                    time.sleep(delay_seconds)
                    
            except Exception as e:
                failed_count += 1
                st.error(f"❌ Error sending to {recipient['business_name']}: {str(e)}")
                
                # Update recipient status as failed
                original_idx = recipient.get('original_index')
                if original_idx is not None:
                    mask = st.session_state.recipients_dataframe['original_index'] == original_idx
                else:
                    mask = st.session_state.recipients_dataframe['business_name'] == recipient['business_name']
                
                st.session_state.recipients_dataframe.loc[mask, 'email_status'] = 'Failed'
                st.session_state.recipients_dataframe.loc[mask, 'sent_date'] = datetime.now().strftime("%Y-%m-%d %H:%M")
                st.session_state.recipients_dataframe.loc[mask, 'campaign_name'] = campaign_name
        
        # Show final results
        progress_bar.progress(1.0)
        status_container.success("🎉 Campaign completed!")
        
        # INTEGRATION FIX: Update main data pipeline with campaign results
        update_main_data_pipeline(st.session_state.recipients_dataframe)
        
        # Force refresh of the recipients table to show updates
        st.rerun()
        
        # Final statistics
        with stats_container:
            col1, col2, col3 = st.columns(3)
            col1.metric("📧 Total", total_recipients)
            col2.metric("✅ Sent", sent_count, delta=f"+{sent_count}")
            col3.metric("❌ Failed", failed_count, delta=f"+{failed_count}" if failed_count > 0 else None)
        
        # Show completion message based on results
        if sent_count > 0:
            st.success(f"🎉 Successfully sent {sent_count} out of {total_recipients} emails!")
            
            if failed_count > 0:
                st.warning(f"⚠️ {failed_count} emails failed to send. Check the recipient table for details.")
            
            st.info("📊 Email status has been updated in the recipients table. You can download the updated data if needed.")
            st.balloons()
            
        else:
            st.error("❌ No emails were sent successfully. Please check your email configuration and try again.")
            
    except Exception as e:
        st.error(f"❌ Campaign execution failed: {str(e)}")
        import traceback
        st.error(f"Debug info: {traceback.format_exc()}")
