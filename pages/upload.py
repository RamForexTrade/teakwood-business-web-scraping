"""
Upload Page
===========
File upload interface with data filtering and download functionality.
"""
import streamlit as st
from state_management import get_state
from controllers import (
    go_to_stage, handle_file_upload, can_proceed_to_map, get_display_dataframe,
    create_download_button, proceed_to_web_research, get_download_stats, validate_proceed_conditions
)
from utils.layout import render_header, render_file_info, render_data_preview, render_success_message, render_filter_controls


def render():
    """Render the upload page."""
    render_header("📤 Upload Data", "Upload your CSV or Excel file with automatic preprocessing and filtering for business research")

    state = get_state()
    
    # Show current file info if any
    if state.uploaded_filename:
        render_file_info()
    
    # File uploader - now supports both CSV and XLSX
    uploaded_file = st.file_uploader(
        "Choose a CSV or Excel file",
        type=["csv", "xlsx"],
        help="Upload a CSV or Excel (.xlsx) file containing business contact data"
    )
    
    # Handle file upload
    if uploaded_file is not None:
        if uploaded_file.name != state.uploaded_filename:
            # New file uploaded
            with st.spinner("Processing file..."):
                success = handle_file_upload(uploaded_file)
            
            if success:
                # Enhanced success message for preprocessing
                file_type = "Excel (.xlsx)" if uploaded_file.name.lower().endswith('.xlsx') else "CSV"
                render_success_message(f"Successfully processed {file_type} file: {uploaded_file.name}")
                st.rerun()
        
        # Show filter controls if we have data
        if state.main_dataframe is not None:
            st.divider()
            render_filter_controls()
            
            st.divider()
            
            # Get the dataframe to display (filtered or main)
            display_df = get_display_dataframe()
            
            # Show filter results info
            if state.filtered_dataframe is not None and (state.primary_filter_values or state.secondary_filter_values):
                original_rows = len(state.main_dataframe)
                filtered_rows = len(display_df)
                filter_ratio = (filtered_rows / original_rows) * 100 if original_rows > 0 else 0
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Original Rows", f"{original_rows:,}")
                with col2:
                    st.metric("Filtered Rows", f"{filtered_rows:,}")
                with col3:
                    st.metric("Retention %", f"{filter_ratio:.1f}%")
                
                if filtered_rows == 0:
                    st.warning("⚠️ No data matches the current filters. Try adjusting your filter criteria.")
                    return
            
            # Show data preview
            render_data_preview(display_df, max_rows=10)
            
            # =================================================================
            # ENHANCED DOWNLOAD AND NAVIGATION SECTION
            # =================================================================
            
            st.divider()
            st.subheader("📥 Download & Continue")
            
            # Download section
            download_col, nav_col = st.columns([1, 1])
            
            with download_col:
                st.write("**Download Current Data:**")
                
                if display_df is not None and len(display_df) > 0:
                    # Get download stats
                    stats = get_download_stats(display_df)
                    
                    # Show download info
                    st.info(f"📊 Ready to download: {stats['rows']:,} rows × {stats['columns']} columns (~{stats['file_size']})")
                    
                    # Determine filename prefix based on filtering
                    if state.primary_filter_values or state.secondary_filter_values:
                        filename_prefix = "filtered_data"
                        button_label = "📥 Download Filtered Data"
                        help_text = "Download the currently filtered dataset as CSV"
                    else:
                        filename_prefix = "original_data" 
                        button_label = "📥 Download Original Data"
                        help_text = "Download the complete uploaded dataset as CSV"
                    
                    # Create download button
                    download_clicked = create_download_button(
                        display_df, 
                        filename_prefix,
                        button_label,
                        help_text
                    )
                    
                    if download_clicked:
                        st.success("✅ Download initiated!")
                
                else:
                    st.warning("No data available for download")
            
            with nav_col:
                st.write("**Proceed to Next Stage:**")
                
                # Validate conditions for proceeding
                can_proceed, error_message = validate_proceed_conditions()
                
                if can_proceed:
                    # Show what data will be used for web research
                    if display_df is not None:
                        st.success(f"✅ Ready to proceed with {len(display_df):,} rows")
                        
                        # Check if we have potential company name columns
                        potential_company_cols = [col for col in display_df.columns 
                                                if any(keyword in col.lower() 
                                                      for keyword in ['name', 'company', 'consignee', 'customer', 'client'])]
                        
                        if potential_company_cols:
                            st.info(f"🏢 Potential company columns found: {', '.join(potential_company_cols[:3])}")
                        else:
                            st.warning("⚠️ No obvious company name columns detected. You can still proceed.")
                    
                    # Proceed button (spec-compliant naming)
                    if st.button("🔍 Proceed to Web Research",
                                width="stretch",
                                type="primary",
                                help="Start web research for contact discovery"):
                        proceed_to_web_research()
                
                else:
                    st.error(f"❌ Cannot proceed: {error_message}")
                    
                    # Disabled button to show expected action
                    st.button("🔍 Proceed to Web Research",
                             width="stretch",
                             disabled=True,
                             help=f"Cannot proceed: {error_message}")
            
            # =================================================================
            # ADDITIONAL INFO SECTION
            # =================================================================
            
            st.divider()
            
            # Show workflow progress
            if state.stage_progress:
                progress_col1, progress_col2 = st.columns([2, 1])
                
                with progress_col1:
                    st.subheader("🚀 Workflow Progress")
                    
                    # Create progress indicators
                    stages = [
                        ("📤 Upload & Filter", state.stage_progress.get('upload', False)),
                        ("🔍 Web Research", state.stage_progress.get('map', False)),
                        ("📧 Email Outreach", state.stage_progress.get('analyze', False))
                    ]
                    
                    for stage_name, completed in stages:
                        if completed:
                            st.success(f"✅ {stage_name}")
                        else:
                            if stage_name.startswith("📤"):
                                st.warning(f"🔄 {stage_name} (Current)")
                            else:
                                st.info(f"⏳ {stage_name} (Pending)")
                
                with progress_col2:
                    if state.session_id:
                        st.write("**Session Info:**")
                        st.code(f"ID: {state.session_id[:8]}...")
                        st.caption("Your work is automatically saved")
    
    else:
        # Show upload instructions
        st.info("👆 Please upload a CSV or Excel (.xlsx) file to continue")
        
        with st.expander("📋 File Format Requirements & Preprocessing Info"):
            st.write("""
            **Supported File Types:**
            - **CSV files** (.csv): Direct upload and processing
            - **Excel files** (.xlsx): Automatically converted to CSV format
            
            **Automatic Preprocessing Features:**
            🔄 **XLSX to CSV Conversion**: Excel files are automatically converted
            📋 **Multi-Sheet Support**: Choose which Excel sheet to process with live preview
            🔍 **Duplicate Removal**: Duplicates based on 'Consignee Name' column are automatically removed
            ✅ **Data Validation**: Automatic checks for data quality and structure
            
            **For best results, your file should contain:**
            - Header row with column names
            - **'Consignee Name' column** (or similar: Company, Business Name, etc.)
            - Contact details (if available)
            - Clean, properly formatted data
            - Categorical columns for filtering (2-50 unique values work best)
            
            **Example Structure:**
            ```
            Consignee Name,Country,Product,Value,Contact Email
            ABC Company,USA,Furniture,10000,
            XYZ Corp,Canada,Timber,15000,info@xyz.com
            DEF Ltd,UK,Plywood,8000,
            ```
            
            **Enhanced Workflow:**
            1. **📤 Upload & Preprocess**: Upload file → Auto-convert XLSX → Remove duplicates → Apply filters
            2. **🔍 Web Research**: Research contact details for companies
            3. **📧 Email Outreach**: Send personalized emails to discovered contacts
            
            **Filter Features:**
            - **Primary Filter**: Choose any categorical column to filter by
            - **Secondary Filter**: Apply text search for more specific results
            - **Real-time Preview**: See filtered data immediately
            - **Download Options**: Download original or filtered data at any stage
            """)
        
        # Show sample data template
        with st.expander("💾 Download Sample Template"):
            st.write("Download a sample template to get started (works for both CSV and Excel):")
            
            # Create sample data with some duplicates to demonstrate preprocessing
            sample_data = {
                'Consignee Name': [
                    'ABC Furniture Co', 'XYZ Timber Ltd', 'DEF Export Inc', 
                    'ABC Furniture Co',  # Duplicate - will be removed
                    'Global Wood Trading', 'Premium Lumber Inc'
                ],
                'Country': ['USA', 'Canada', 'UK', 'USA', 'Germany', 'Australia'],
                'Product Description': [
                    'Teak Wood Furniture', 'Pine Lumber', 'Plywood Sheets',
                    'Oak Furniture',  # Different product but same company (will be removed)
                    'Bamboo Products', 'Hardwood Flooring'
                ],
                'Value USD': [15000, 25000, 18000, 12000, 30000, 22000],
                'Contact Email': ['', 'info@xyztimber.com', '', '', 'sales@globalwood.de', ''],
                'Phone': ['', '+1-555-0123', '', '', '+49-30-123456', '+61-2-9876543'],
            }
            
            import pandas as pd
            sample_df = pd.DataFrame(sample_data)
            
            st.info("📊 This sample includes duplicate 'ABC Furniture Co' entries to demonstrate automatic duplicate removal.")
            
            create_download_button(
                sample_df, 
                "sample_template_with_duplicates",
                "📥 Download Sample Template (CSV)",
                "Download a sample template with duplicates to test preprocessing features"
            )
    
    # Debug info for development
    if state.show_debug and state.main_dataframe is not None:
        with st.expander("🔍 Enhanced Debug Info"):
            df = state.main_dataframe
            display_df = get_display_dataframe()
            
            st.write(f"**Original Shape:** {df.shape}")
            st.write(f"**Display Shape:** {display_df.shape if display_df is not None else 'None'}")
            st.write(f"**Working Data Shape:** {state.working_data.shape if state.working_data is not None else 'None'}")
            st.write(f"**Columns:** {list(df.columns)}")
            st.write(f"**Data types:**")
            st.write(df.dtypes)
            
            if state.primary_filter_values or state.secondary_filter_values:
                st.write(f"**Active Filters:**")
                if state.primary_filter_values:
                    st.write(f"- Primary: {state.primary_filter_column} = {state.primary_filter_values}")
                if state.secondary_filter_values:
                    st.write(f"- Secondary: {state.secondary_filter_column} = {state.secondary_filter_values}")
            
            # Show session info
            st.write("**Session Info:**")
            st.json({
                "session_id": state.session_id,
                "stage_progress": state.stage_progress,
                "data_history_length": len(state.data_history),
                "filters_applied": state.filters_applied
            })
