"""
Controllers Module
==================
State transitions and control flow logic for the application.
"""
import streamlit as st
import pandas as pd
from typing import Any, Optional
from datetime import datetime
import io
import os
from io import BytesIO
from state_management import get_state, update_state, add_data_checkpoint, update_stage_progress


def go_to_stage(stage_name: str) -> None:
    """Navigate to a specific stage/page."""
    valid_stages = ["upload", "map", "analyze", "ai_chat", "visualizations"]

    if stage_name not in valid_stages:
        st.error(f"Invalid stage: {stage_name}. Valid stages: {valid_stages}")
        return

    # CLOUD DEPLOYMENT FIX: Update both state management and session state
    update_state(current_stage=stage_name)
    st.session_state.current_stage = stage_name  # Ensure session state is also updated
    st.rerun()


def save_uploaded_file(uploaded_file: Any) -> None:
    """Save uploaded file to state."""
    update_state(
        uploaded_file=uploaded_file,
        uploaded_filename=uploaded_file.name if uploaded_file else ""
    )


def save_dataframe(df: Any) -> None:
    """Save main dataframe to state."""
    update_state(main_dataframe=df)


def save_analysis_results(results: dict) -> None:
    """Save analysis results to state."""
    update_state(analysis_results=results)


def can_proceed_to_map() -> bool:
    """Check if user can proceed to map stage."""
    state = get_state()
    return (state.uploaded_file is not None and 
            state.main_dataframe is not None)


def can_proceed_to_analyze() -> bool:
    """Check if user can proceed to analyze stage."""
    state = get_state()
    return state.main_dataframe is not None


def handle_file_upload(uploaded_file: Any) -> bool:
    """Enhanced file upload workflow with XLSX support, sheet selection, and duplicate removal."""
    from utils.data_utils import clean_dataframe_for_arrow
    
    if uploaded_file is None:
        return False
    
    try:
        # Try to import preprocessor, fallback to basic loading if not available
        try:
            from services.preprocessor import preprocess_uploaded_file, validate_preprocessed_data, show_preprocessing_summary, get_excel_sheet_names
            use_preprocessing = True
        except ImportError as e:
            st.warning(f"⚠️ Preprocessing module not available: {str(e)}. Using basic file loading.")
            use_preprocessing = False
        
        # Convert UploadedFile to bytes
        file_bytes = uploaded_file.getvalue()
        filename = uploaded_file.name
        
        # Check if we need to handle Excel sheet selection
        selected_sheet = None
        if use_preprocessing and filename.lower().endswith('.xlsx'):
            # Check if this is a sheet selection scenario
            if f"sheet_selection_{uploaded_file.name}" in st.session_state:
                selected_sheet = st.session_state[f"sheet_selection_{uploaded_file.name}"]
                # Clear the session state after use
                del st.session_state[f"sheet_selection_{uploaded_file.name}"]
        
        if use_preprocessing:
            # PREPROCESSING PATH: Handle XLSX conversion and duplicate removal
            st.info("🔄 Starting file preprocessing...")
            
            success, processed_df, summary_message = preprocess_uploaded_file(file_bytes, filename, selected_sheet)
            
            if not success:
                # Check if this is a multi-sheet Excel file requiring user selection
                if summary_message.startswith("MULTI_SHEET:"):
                    sheet_names = summary_message.replace("MULTI_SHEET:", "").split(",")
                    
                    st.info(f"📊 Excel file contains {len(sheet_names)} sheets. Please select which sheet to process:")
                    
                    # Show sheet selection interface
                    with st.container():
                        st.subheader("📋 Sheet Selection")
                        
                        # Display available sheets with preview info if possible
                        col1, col2 = st.columns([1, 2])
                        
                        with col1:
                            selected_sheet_choice = st.selectbox(
                                "Choose a sheet to process:",
                                options=sheet_names,
                                key=f"sheet_selector_{filename}",
                                help="Select the Excel sheet that contains your data"
                            )
                        
                        with col2:
                            # Try to show preview of selected sheet
                            if selected_sheet_choice:
                                try:
                                    file_io = BytesIO(file_bytes)
                                    preview_df = pd.read_excel(file_io, sheet_name=selected_sheet_choice, nrows=3)
                                    
                                    st.write(f"**Preview of '{selected_sheet_choice}' sheet:**")
                                    st.dataframe(preview_df, width="stretch")
                                    st.caption(f"Sheet has {len(preview_df.columns)} columns. Preview shows first 3 rows.")
                                    
                                except Exception as e:
                                    st.warning(f"Could not preview sheet: {str(e)}")
                        
                        # Process button
                        if st.button(
                            f"🚀 Process Selected Sheet: '{selected_sheet_choice}'",
                            type="primary",
                            width="stretch",
                            key=f"process_sheet_{filename}"
                        ):
                            # Store the selected sheet and rerun
                            st.session_state[f"sheet_selection_{uploaded_file.name}"] = selected_sheet_choice
                            st.rerun()
                    
                    return False  # Don't proceed yet, wait for sheet selection
                
                else:
                    st.error(f"Preprocessing failed: {summary_message}")
                    # Fallback to basic loading
                    st.info("🔄 Attempting basic file loading...")
                    use_preprocessing = False
            else:
                df = processed_df
        
        if not use_preprocessing:
            # FALLBACK PATH: Basic CSV loading (original functionality)
            st.info("📄 Loading file using basic method...")
            
            if filename.lower().endswith('.xlsx'):
                st.error("❌ Excel files require the preprocessing module. Please ensure all dependencies are installed.")
                return False
            
            # Import here to avoid circular imports
            from services.data_loader import load_csv
            df = load_csv(file_bytes)
            
            if df.empty:
                st.error("Uploaded file is empty or could not be processed")
                return False
        
        # Validate and clean dataframe
        if df is None or df.empty:
            st.error("No data available after processing")
            return False
        
        # Additional cleaning for safety
        cleaned_df = clean_dataframe_for_arrow(df)
        
        # Validate dataframe before saving
        if len(cleaned_df.columns) == 0:
            st.error("No valid columns found in the processed file")
            return False
        
        # Data validation (if preprocessing is available)
        if use_preprocessing:
            is_valid, warnings = validate_preprocessed_data(cleaned_df)
            
            if warnings:
                st.warning("⚠️ Data validation warnings:")
                for warning in warnings:
                    st.warning(f"• {warning}")
            
            if not is_valid:
                st.error("❌ Processed data has critical issues. Please check your file.")
                return False
        
        # Save to state
        save_uploaded_file(uploaded_file)
        save_dataframe(cleaned_df)
        
        # Initialize session data for workflow
        initialize_session_on_upload(cleaned_df)
        
        # Show results
        if use_preprocessing:
            # Show preprocessing results
            st.divider()
            st.subheader("📋 Preprocessing Results")
            
            # Show detailed summary
            show_preprocessing_summary(df, cleaned_df, 
                                     "xlsx" if filename.lower().endswith('.xlsx') else "csv")
            
            if selected_sheet:
                st.info(f"📊 Processed sheet: '{selected_sheet}'")
            
            st.success(f"✅ File successfully processed! Final dataset: {len(cleaned_df)} rows × {len(cleaned_df.columns)} columns")
            
            # Show sample of processed data
            with st.expander("👀 Preview Processed Data"):
                st.dataframe(cleaned_df.head(10), width="stretch")
        else:
            # Basic success message
            st.success(f"✅ File loaded successfully! Dataset: {len(cleaned_df)} rows × {len(cleaned_df.columns)} columns")
        
        return True
        
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
        st.info("💡 If you're uploading an Excel file, make sure all dependencies are properly installed.")
        return False


# ============================================================================
# NEW FUNCTIONS FOR STAGE 3 IMPLEMENTATION
# ============================================================================

def initialize_session_on_upload(df: pd.DataFrame) -> None:
    """Initialize session state when file is uploaded - FIXED to preserve email status."""
    try:
        state = get_state()
        
        # CRITICAL FIX: Preserve existing email status data if it exists in the uploaded CSV
        preserved_df = preserve_email_status_from_csv(df)
        
        # Set original dataframe as backup with preserved email status
        update_state(original_dataframe=preserved_df.copy())
        
        # Create initial data checkpoint
        add_data_checkpoint("File uploaded with email status preserved", preserved_df)
        
        # Initialize working data with preserved email status
        update_state(working_data=preserved_df.copy())
        
        # FIXED: Only reset stage progress if this is truly a new upload
        # Check if we're reloading the same file with email status
        if has_email_status_columns(df):
            # Keep existing progress if email status exists (indicates continued workflow)
            st.info("📧 Email status columns detected - preserving workflow progress")
        else:
            # Reset stage progress for new uploads
            update_state(stage_progress={
                'upload': False,
                'map': False, 
                'analyze': False
            })
        
        # Reset filters (but preserve data)
        update_state(
            primary_filter_column="",
            primary_filter_values=[],
            secondary_filter_column="",
            secondary_filter_values=[],
            filtered_dataframe=None,
            filters_applied={}
        )
        
        # CRITICAL FIX: Sync enhanced_data with email status if it exists
        if 'enhanced_data' in st.session_state and st.session_state.enhanced_data is not None:
            st.session_state.enhanced_data = sync_email_status_to_enhanced_data(
                st.session_state.enhanced_data, 
                preserved_df
            )
        
    except Exception as e:
        st.error(f"Error initializing session: {str(e)}")


def create_download_button(df: pd.DataFrame, filename_prefix: str, 
                          label: str = "📥 Download CSV", 
                          help_text: str = "Download the current data as CSV") -> bool:
    """
    Create a download button for CSV data.
    
    Args:
        df: DataFrame to download
        filename_prefix: Prefix for the filename
        label: Button label
        help_text: Help text for the button
        
    Returns:
        bool: True if download button was clicked
    """
    try:
        if df is None or len(df) == 0:
            st.warning("No data available for download")
            return False
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.csv"
        
        # Convert dataframe to CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()
        
        # Create download button
        download_clicked = st.download_button(
            label=label,
            data=csv_string,
            file_name=filename,
            mime="text/csv",
            help=help_text,
            width="stretch"
        )
        
        return download_clicked
        
    except Exception as e:
        st.error(f"Error creating download button: {str(e)}")
        return False


def save_filtered_data_to_session(filtered_df: pd.DataFrame) -> bool:
    """
    Save filtered data to session state and create checkpoint.
    
    Args:
        filtered_df: Filtered DataFrame to save
        
    Returns:
        bool: Success status
    """
    try:
        state = get_state()
        
        # Update working data with filtered data
        update_state(working_data=filtered_df.copy())
        
        # Create data checkpoint
        filter_description = create_filter_description()
        add_data_checkpoint(f"Data filtered: {filter_description}", filtered_df)
        
        # Save filters applied
        filters_applied = {
            "primary_column": state.primary_filter_column,
            "primary_values": state.primary_filter_values,
            "secondary_column": state.secondary_filter_column,
            "secondary_values": state.secondary_filter_values,
            "applied_timestamp": datetime.now().isoformat(),
            "original_rows": len(state.main_dataframe) if state.main_dataframe is not None else 0,
            "filtered_rows": len(filtered_df)
        }
        
        update_state(filters_applied=filters_applied)
        
        # Save session data to file system for persistence
        save_session_data_to_file(filtered_df, "filtered_data")
        
        return True
        
    except Exception as e:
        st.error(f"Error saving filtered data to session: {str(e)}")
        return False


def save_session_data_to_file(df: pd.DataFrame, stage: str) -> bool:
    """
    Save session data to file system for persistence.
    
    Args:
        df: DataFrame to save
        stage: Stage identifier
        
    Returns:
        bool: Success status
    """
    try:
        state = get_state()
        
        # Create session directory path
        session_dir = f"temp_files/session_{state.session_id}/data"
        os.makedirs(session_dir, exist_ok=True)
        
        # Create filename with stage and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{stage}_{timestamp}.csv"
        filepath = os.path.join(session_dir, filename)
        
        # Save to CSV
        df.to_csv(filepath, index=False)
        
        return True
        
    except Exception as e:
        st.warning(f"Could not save session data to file: {str(e)}")
        return False


def create_filter_description() -> str:
    """Create a human-readable description of applied filters."""
    try:
        state = get_state()
        descriptions = []
        
        if state.primary_filter_column and state.primary_filter_values:
            descriptions.append(f"{state.primary_filter_column} ({len(state.primary_filter_values)} values)")
        
        if state.secondary_filter_column and state.secondary_filter_values:
            descriptions.append(f"{state.secondary_filter_column} ({len(state.secondary_filter_values)} values)")
        
        if descriptions:
            return " + ".join(descriptions)
        else:
            return "No filters applied"
            
    except Exception as e:
        return "Filter description error"


def proceed_to_web_research() -> None:
    """
    ENHANCED: Handle transition to web research stage with filtered data consistency.
    
    CRITICAL FIX: Ensures that only filtered data (if filters applied) is used for web research.
    """
    try:
        state = get_state()
        
        # CRITICAL: Get the actual display dataframe (filtered or main)
        display_df = get_display_dataframe()
        
        if display_df is None or len(display_df) == 0:
            st.error("❌ No data available to proceed to web research")
            return
        
        # ENHANCED: Determine if we're using filtered data
        using_filtered_data = (state.filtered_dataframe is not None and 
                              not state.filtered_dataframe.empty and
                              (bool(state.primary_filter_values) or bool(state.secondary_filter_values)))
        
        # CRITICAL: Set working_data to the display dataframe
        update_state(working_data=display_df.copy())
        
        # ENHANCED: Update session state for cross-stage consistency
        st.session_state.working_data = display_df.copy()
        
        # If using filtered data, also preserve the filtered dataframe
        if using_filtered_data:
            st.session_state.filtered_data_for_research = display_df.copy()
            st.session_state.research_uses_filtered_data = True
            
            # Add filter info to session
            st.session_state.filter_info = {
                'primary_column': state.primary_filter_column,
                'primary_values': state.primary_filter_values,
                'secondary_column': state.secondary_filter_column,
                'secondary_values': state.secondary_filter_values,
                'original_rows': len(state.main_dataframe) if state.main_dataframe is not None and not state.main_dataframe.empty else 0,
                'filtered_rows': len(display_df),
                'filter_timestamp': datetime.now().isoformat()
            }
            
            st.success(f"✅ Filtered dataset ready for web research: {len(display_df):,} rows")
        else:
            st.session_state.research_uses_filtered_data = False
            st.success(f"✅ Complete dataset ready for web research: {len(display_df):,} rows")
        
        # Save current data to session file
        success = save_filtered_data_to_session(display_df)
        
        if success:
            # Mark upload stage as completed
            update_stage_progress('upload', True)
            
            # Add checkpoint for stage completion
            data_type = "filtered" if using_filtered_data else "complete"
            add_data_checkpoint(f"Upload stage completed - proceeding to web research with {data_type} dataset", display_df)
            
            # ENHANCED: Add research preparation metadata
            research_metadata = {
                'data_rows': len(display_df),
                'data_columns': len(display_df.columns),
                'using_filtered_data': using_filtered_data,
                'preparation_timestamp': datetime.now().isoformat(),
                'company_column_candidates': find_company_columns(display_df)
            }
            
            st.session_state.research_metadata = research_metadata
            
            # Navigate to map stage (web research)
            # CLOUD DEPLOYMENT FIX: Update both state management and session state
            update_state(current_stage="map")
            st.session_state.current_stage = "map"  # Ensure session state is also updated

            st.success("✅ Upload stage completed! Proceeding to Web Research...")
            st.rerun()
        else:
            st.error("❌ Failed to save session data. Please try again.")
            
    except Exception as e:
        st.error(f"❌ Error proceeding to web research: {str(e)}")


def find_company_columns(df: pd.DataFrame) -> list:
    """Helper function to identify potential company name columns."""
    company_keywords = ['name', 'company', 'consignee', 'business', 'customer', 'client', 'vendor']
    potential_columns = []
    
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in company_keywords):
            potential_columns.append(col)
    
    return potential_columns


def apply_filters_enhanced():
    """
    ENHANCED: Apply filters and update working data consistently.
    """
    try:
        # Apply filters to create filtered_dataframe
        apply_filters()  # Original function
        
        # CRITICAL: Update working data to match filtered view
        display_df = get_display_dataframe()
        if display_df is not None:
            update_state(working_data=display_df.copy())
            st.session_state.working_data = display_df.copy()
        
        return True
        
    except Exception as e:
        st.error(f"Error applying enhanced filters: {str(e)}")
        return False


def export_stage_data(df: pd.DataFrame, stage: str) -> str:
    """
    Export data for a specific stage and return file path.
    
    Args:
        df: DataFrame to export
        stage: Stage identifier
        
    Returns:
        str: File path of exported data
    """
    try:
        state = get_state()
        
        # Create exports directory
        exports_dir = f"temp_files/session_{state.session_id}/exports"
        os.makedirs(exports_dir, exist_ok=True)
        
        # Create filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"export_{stage}_{timestamp}.csv"
        filepath = os.path.join(exports_dir, filename)
        
        # Export to CSV
        df.to_csv(filepath, index=False)
        
        return filepath
        
    except Exception as e:
        st.error(f"Error exporting stage data: {str(e)}")
        return ""


# ============================================================================
# EXISTING FUNCTIONS (ENHANCED)
# ============================================================================

def trigger_analysis() -> None:
    """Trigger data analysis workflow."""
    state = get_state()
    
    # Get the dataframe to analyze (filtered or main)
    df_to_analyze = get_display_dataframe()
    
    if df_to_analyze is None or len(df_to_analyze) == 0:
        st.error("No data available for analysis")
        return
    
    try:
        # Import here to avoid circular imports
        from services.compute import analyze_data
        
        # Perform analysis on the display dataframe (filtered or main)
        results = analyze_data(df_to_analyze)
        
        # Save results
        save_analysis_results(results)
        
        # Navigate to results
        go_to_stage("analyze")
        
    except Exception as e:
        st.error(f"Error during analysis: {str(e)}")


def reset_app() -> None:
    """Reset the entire application state."""
    from state_management import reset_state
    reset_state()
    st.rerun()


def apply_filters() -> None:
    """Apply current filters to the main dataframe and update filtered_dataframe."""
    from utils.data_utils import clean_dataframe_for_arrow
    
    state = get_state()
    
    if state.main_dataframe is None:
        return
    
    df = state.main_dataframe.copy()
    
    # Apply primary filter
    if state.primary_filter_column and state.primary_filter_values:
        if state.primary_filter_column in df.columns:
            # Convert to string for safe comparison
            df_col_str = df[state.primary_filter_column].astype(str)
            filter_values_str = [str(v) for v in state.primary_filter_values]
            df = df[df_col_str.isin(filter_values_str)]
    
    # Apply secondary filter
    if state.secondary_filter_column and state.secondary_filter_values:
        if state.secondary_filter_column in df.columns:
            # Convert to string for safe comparison
            df_col_str = df[state.secondary_filter_column].astype(str)
            filter_values_str = [str(v) for v in state.secondary_filter_values]
            df = df[df_col_str.isin(filter_values_str)]
    
    # Clean the filtered dataframe for Arrow compatibility
    cleaned_df = clean_dataframe_for_arrow(df)
    
    # Update filtered dataframe
    update_state(filtered_dataframe=cleaned_df)


def reset_filters() -> None:
    """Reset all filters to their default state."""
    update_state(
        primary_filter_column="",
        primary_filter_values=[],
        secondary_filter_column="",
        secondary_filter_values=[],
        filtered_dataframe=None
    )


def get_filterable_columns() -> list:
    """Get list of columns suitable for filtering (categorical/text columns)."""
    from utils.data_utils import get_filterable_columns_safe
    
    state = get_state()
    
    if state.main_dataframe is None:
        return []
    
    return get_filterable_columns_safe(state.main_dataframe)


def get_column_unique_values(column: str) -> list:
    """Get unique values for a specific column."""
    from utils.data_utils import safe_unique_values
    
    state = get_state()
    
    if state.main_dataframe is None or column not in state.main_dataframe.columns:
        return []
    
    return safe_unique_values(state.main_dataframe, column)


def get_display_dataframe():
    """Get the dataframe to display (filtered if filters applied, otherwise main)."""
    state = get_state()
    
    # Return filtered dataframe if filters are applied
    if (state.filtered_dataframe is not None and 
        not state.filtered_dataframe.empty and
        (bool(state.primary_filter_values) or bool(state.secondary_filter_values))):
        return state.filtered_dataframe
    
    # Otherwise return main dataframe
    return state.main_dataframe


def show_debug_info() -> None:
    """Show debug information about current state."""
    from state_management import get_state_summary
    
    with st.expander("🐛 Debug Info"):
        st.json(get_state_summary())


# ============================================================================
# UTILITY FUNCTIONS FOR ENHANCED FUNCTIONALITY
# ============================================================================

def get_download_stats(df: pd.DataFrame) -> dict:
    """Get statistics for download summary."""
    try:
        if df is None or len(df) == 0:
            return {"rows": 0, "columns": 0, "file_size": "0 KB"}
        
        # Calculate approximate file size
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        file_size_bytes = len(csv_buffer.getvalue().encode('utf-8'))
        
        if file_size_bytes < 1024:
            file_size = f"{file_size_bytes} B"
        elif file_size_bytes < 1024 * 1024:
            file_size = f"{file_size_bytes / 1024:.1f} KB"
        else:
            file_size = f"{file_size_bytes / (1024 * 1024):.1f} MB"
        
        return {
            "rows": len(df),
            "columns": len(df.columns),
            "file_size": file_size
        }
        
    except Exception as e:
        return {"rows": 0, "columns": 0, "file_size": "Error calculating size"}


def validate_proceed_conditions() -> tuple[bool, str]:
    """
    Validate conditions for proceeding to next stage.
    
    Returns:
        tuple: (can_proceed, error_message)
    """
    try:
        state = get_state()
        
        # Check if we have data
        if state.main_dataframe is None:
            return False, "No data uploaded"
        
        working_df = get_display_dataframe()
        
        if working_df is None or len(working_df) == 0:
            return False, "No data available after filtering"
        
        # Check if we have required columns for web research (e.g., company names)
        # This can be enhanced based on specific requirements
        if len(working_df.columns) == 0:
            return False, "No columns available in the data"
        
        return True, ""
        
    except Exception as e:
        return False, f"Error validating conditions: {str(e)}"


# ============================================================================
# EMAIL STATUS PRESERVATION FUNCTIONS - CRITICAL FIX
# ============================================================================

def preserve_email_status_from_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    CRITICAL FIX: Preserve email status columns from uploaded CSV file.
    
    Args:
        df: DataFrame from uploaded CSV
        
    Returns:
        DataFrame with preserved email status columns
    """
    try:
        preserved_df = df.copy()
        
        # Email status columns to look for and preserve
        email_status_columns = [
            'email_selected', 'email_status', 'sent_date', 'campaign_name',
            'Email Selected', 'Email Status', 'Sent Date', 'Campaign Name',
            'Email_Selected', 'Email_Status', 'Sent_Date', 'Campaign_Name'
        ]
        
        # Check if any email status columns exist in the CSV
        found_email_columns = [col for col in preserved_df.columns if col in email_status_columns]
        
        if found_email_columns:
            st.success(f"📧 Email status columns preserved: {', '.join(found_email_columns)}")
            
            # Standardize column names to match expected format
            column_mapping = {
                'Email Selected': 'email_selected',
                'Email Status': 'email_status',
                'Sent Date': 'sent_date',
                'Campaign Name': 'campaign_name',
                'Email_Selected': 'email_selected',
                'Email_Status': 'email_status',
                'Sent_Date': 'sent_date',
                'Campaign_Name': 'campaign_name'
            }
            
            # Rename columns to standard format
            for old_name, new_name in column_mapping.items():
                if old_name in preserved_df.columns:
                    preserved_df = preserved_df.rename(columns={old_name: new_name})
            
            # Ensure data types are correct
            if 'email_selected' in preserved_df.columns:
                # Convert to boolean, handling various formats
                preserved_df['email_selected'] = preserved_df['email_selected'].apply(
                    lambda x: str(x).lower() in ['true', '1', 'yes', 'y'] if pd.notna(x) else False
                )
            
            if 'email_status' in preserved_df.columns:
                # Fill empty status with 'Not Sent'
                preserved_df['email_status'] = preserved_df['email_status'].fillna('Not Sent')
                preserved_df['email_status'] = preserved_df['email_status'].replace('', 'Not Sent')
            
            if 'sent_date' in preserved_df.columns:
                # Fill empty dates
                preserved_df['sent_date'] = preserved_df['sent_date'].fillna('')
                preserved_df['sent_date'] = preserved_df['sent_date'].replace('nan', '')
            
            if 'campaign_name' in preserved_df.columns:
                # Fill empty campaign names
                preserved_df['campaign_name'] = preserved_df['campaign_name'].fillna('')
                preserved_df['campaign_name'] = preserved_df['campaign_name'].replace('nan', '')
        
        else:
            # Add default email status columns if they don't exist
            preserved_df['email_selected'] = False
            preserved_df['email_status'] = 'Not Sent'
            preserved_df['sent_date'] = ''
            preserved_df['campaign_name'] = ''
            st.info("📧 Added default email status columns for new workflow")
        
        return preserved_df
        
    except Exception as e:
        st.warning(f"⚠️ Error preserving email status: {str(e)}")
        # Return original dataframe with default columns
        df['email_selected'] = False
        df['email_status'] = 'Not Sent'
        df['sent_date'] = ''
        df['campaign_name'] = ''
        return df


def has_email_status_columns(df: pd.DataFrame) -> bool:
    """
    Check if DataFrame has email status columns indicating continued workflow.
    
    Args:
        df: DataFrame to check
        
    Returns:
        bool: True if email status columns are found
    """
    try:
        email_status_columns = [
            'email_selected', 'email_status', 'sent_date', 'campaign_name',
            'Email Selected', 'Email Status', 'Sent Date', 'Campaign Name',
            'Email_Selected', 'Email_Status', 'Sent_Date', 'Campaign_Name'
        ]
        
        # Check if any email status columns exist
        found_columns = [col for col in df.columns if col in email_status_columns]
        
        # Also check if any of these columns have non-default values indicating actual usage
        if found_columns:
            for col in found_columns:
                if col in df.columns:
                    # Check for non-default values
                    if col.lower().replace('_', '').replace(' ', '') in ['emailselected']:
                        if df[col].any():  # Any True values
                            return True
                    elif col.lower().replace('_', '').replace(' ', '') in ['emailstatus']:
                        non_default_statuses = df[col][df[col].notna() & (df[col] != '') & (df[col] != 'Not Sent')]
                        if len(non_default_statuses) > 0:
                            return True
                    elif col.lower().replace('_', '').replace(' ', '') in ['sentdate']:
                        non_empty_dates = df[col][df[col].notna() & (df[col] != '')]
                        if len(non_empty_dates) > 0:
                            return True
                    elif col.lower().replace('_', '').replace(' ', '') in ['campaignname']:
                        non_empty_campaigns = df[col][df[col].notna() & (df[col] != '')]
                        if len(non_empty_campaigns) > 0:
                            return True
        
        return False
        
    except Exception as e:
        return False


def sync_email_status_to_enhanced_data(enhanced_data: pd.DataFrame, csv_data: pd.DataFrame) -> pd.DataFrame:
    """
    Sync email status from CSV data to enhanced data based on matching rows.
    
    Args:
        enhanced_data: Enhanced data from business research
        csv_data: CSV data with email status
        
    Returns:
        Updated enhanced data with email status
    """
    try:
        if enhanced_data is None or csv_data is None:
            return enhanced_data
        
        # Add email status columns to enhanced data if they don't exist
        email_columns = ['email_selected', 'email_status', 'sent_date', 'campaign_name']
        for col in email_columns:
            if col not in enhanced_data.columns:
                if col == 'email_selected':
                    enhanced_data[col] = False
                else:
                    enhanced_data[col] = ''
        
        # Try to match rows between enhanced_data and csv_data
        # Common matching strategies: by business name, email, or index
        
        # Strategy 1: Match by business name if available
        business_name_cols = [col for col in enhanced_data.columns if 'name' in col.lower() and 'business' in col.lower()]
        csv_business_cols = [col for col in csv_data.columns if 'name' in col.lower()]
        
        if business_name_cols and csv_business_cols:
            enhanced_business_col = business_name_cols[0]
            csv_business_col = csv_business_cols[0]
            
            # Match by business name
            for idx, enhanced_row in enhanced_data.iterrows():
                business_name = enhanced_row[enhanced_business_col]
                
                # Find matching row in CSV data
                csv_matches = csv_data[csv_data[csv_business_col] == business_name]
                
                if len(csv_matches) > 0:
                    csv_row = csv_matches.iloc[0]
                    
                    # Copy email status columns
                    for col in email_columns:
                        if col in csv_row:
                            enhanced_data.loc[idx, col] = csv_row[col]
        
        # Strategy 2: Match by index if business name matching fails
        else:
            # Match by index position
            for idx in enhanced_data.index:
                if idx < len(csv_data):
                    csv_row = csv_data.iloc[idx]
                    
                    # Copy email status columns
                    for col in email_columns:
                        if col in csv_data.columns:
                            enhanced_data.loc[idx, col] = csv_row[col]
        
        st.info("🔄 Email status synchronized to enhanced data")
        return enhanced_data
        
    except Exception as e:
        st.warning(f"⚠️ Could not sync email status to enhanced data: {str(e)}")
        return enhanced_data
