"""
Data Preprocessing Service
=========================
Handles XLSX to CSV conversion and duplicate removal based on Consignee Name.
"""
import pandas as pd
import streamlit as st
from io import BytesIO
from typing import Optional, Tuple
import os

from utils.data_utils import clean_dataframe_for_arrow


def get_excel_sheet_names(file_bytes: bytes) -> list:
    """
    Get list of sheet names from an Excel file.
    
    Args:
        file_bytes: Excel file content as bytes
        
    Returns:
        List of sheet names or empty list if error
    """
    try:
        file_io = BytesIO(file_bytes)
        excel_file = pd.ExcelFile(file_io)
        return excel_file.sheet_names
    except Exception as e:
        st.warning(f"Error reading Excel file sheets: {str(e)}")
        return []


def detect_file_type(file_bytes: bytes, filename: str) -> str:
    """
    Detect the file type based on filename and content.
    
    Args:
        file_bytes: File content as bytes
        filename: Original filename
        
    Returns:
        str: File type ('xlsx', 'csv', 'unknown')
    """
    try:
        # Check file extension
        if filename.lower().endswith('.xlsx'):
            return 'xlsx'
        elif filename.lower().endswith('.csv'):
            return 'csv'
        else:
            # Try to detect by content
            try:
                # Try to read as Excel first
                BytesIO(file_bytes).seek(0)
                pd.read_excel(BytesIO(file_bytes), nrows=1)
                return 'xlsx'
            except:
                try:
                    # Try to read as CSV
                    BytesIO(file_bytes).seek(0)
                    pd.read_csv(BytesIO(file_bytes), nrows=1)
                    return 'csv'
                except:
                    return 'unknown'
    except Exception as e:
        st.warning(f"Error detecting file type: {str(e)}")
        return 'unknown'


def convert_xlsx_to_csv(file_bytes: bytes, filename: str, selected_sheet: str = None) -> Tuple[bool, Optional[pd.DataFrame], str]:
    """
    Convert XLSX file to CSV format (as DataFrame) with optional sheet selection.
    
    Args:
        file_bytes: XLSX file content as bytes
        filename: Original filename
        selected_sheet: Specific sheet name to use (optional)
        
    Returns:
        Tuple of (success, dataframe, message)
    """
    try:
        # Create BytesIO object for pandas
        file_io = BytesIO(file_bytes)
        
        # Try to read Excel file
        try:
            # First, try to get all sheet names
            excel_file = pd.ExcelFile(file_io)
            sheet_names = excel_file.sheet_names
            
            if len(sheet_names) == 0:
                return False, None, "No sheets found in Excel file"
            
            # Determine which sheet to use
            if selected_sheet and selected_sheet in sheet_names:
                sheet_to_use = selected_sheet
                st.info(f"📊 Using selected sheet: '{sheet_to_use}'")
            elif len(sheet_names) == 1:
                sheet_to_use = sheet_names[0]
                st.info(f"📊 Single sheet found: '{sheet_to_use}'")
            else:
                # Multiple sheets but no selection - return sheet names for user selection
                if selected_sheet is None:
                    return False, None, f"MULTI_SHEET:{','.join(sheet_names)}"
                else:
                    return False, None, f"Selected sheet '{selected_sheet}' not found. Available sheets: {', '.join(sheet_names)}"
            
            # Read the selected sheet
            df = pd.read_excel(file_io, sheet_name=sheet_to_use)
            
            # Validate the dataframe
            if df.empty:
                return False, None, f"Sheet '{sheet_to_use}' is empty"
            
            if len(df.columns) == 0:
                return False, None, f"Sheet '{sheet_to_use}' has no columns"
            
            # Clean the dataframe
            cleaned_df = clean_dataframe_for_arrow(df)
            
            success_msg = f"✅ Successfully converted XLSX to CSV format. Sheet: '{sheet_to_use}', Rows: {len(cleaned_df)}, Columns: {len(cleaned_df.columns)}"
            
            return True, cleaned_df, success_msg
            
        except Exception as e:
            return False, None, f"Error reading Excel file: {str(e)}"
            
    except Exception as e:
        return False, None, f"Error converting XLSX to CSV: {str(e)}"


def remove_duplicates_by_consignee(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    Remove duplicates based on 'Consignee Name' column.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (deduplicated_dataframe, summary_message)
    """
    try:
        if df is None or df.empty:
            return df, "No data to process"
        
        original_count = len(df)
        
        # Find the Consignee Name column (case-insensitive search)
        consignee_column = None
        possible_columns = [
            'Consignee Name', 'consignee name', 'CONSIGNEE NAME',
            'Consignee_Name', 'consignee_name', 'CONSIGNEE_NAME',
            'ConsigneeName', 'consigneename', 'CONSIGNEENAME'
        ]
        
        # Check for exact matches first
        for col in possible_columns:
            if col in df.columns:
                consignee_column = col
                break
        
        # If no exact match, try partial matching
        if consignee_column is None:
            for col in df.columns:
                col_lower = col.lower().replace(' ', '').replace('_', '')
                if 'consignee' in col_lower and 'name' in col_lower:
                    consignee_column = col
                    break
        
        # If still no match, try broader search
        if consignee_column is None:
            for col in df.columns:
                col_lower = col.lower()
                if 'consignee' in col_lower:
                    consignee_column = col
                    st.info(f"Using column '{col}' for duplicate removal (closest match to 'Consignee Name')")
                    break
        
        # If no consignee column found, try company/name columns
        if consignee_column is None:
            name_keywords = ['company', 'business', 'name', 'customer', 'client']
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in name_keywords):
                    consignee_column = col
                    st.warning(f"⚠️ 'Consignee Name' column not found. Using '{col}' for duplicate removal.")
                    break
        
        if consignee_column is None:
            warning_msg = "⚠️ No suitable column found for duplicate removal. Columns available: " + ", ".join(df.columns)
            st.warning(warning_msg)
            return df, f"No duplicate removal performed. {warning_msg}"
        
        # Remove duplicates based on the identified column
        # Keep the first occurrence of each duplicate
        df_deduplicated = df.drop_duplicates(subset=[consignee_column], keep='first')
        
        duplicates_removed = original_count - len(df_deduplicated)
        
        if duplicates_removed > 0:
            success_msg = f"✅ Duplicate removal completed. Removed {duplicates_removed} duplicate(s) based on '{consignee_column}'. Original: {original_count} rows → Final: {len(df_deduplicated)} rows"
            st.success(success_msg)
        else:
            success_msg = f"✅ No duplicates found in '{consignee_column}'. Dataset remains unchanged with {len(df_deduplicated)} rows"
            st.info(success_msg)
        
        return df_deduplicated, success_msg
        
    except Exception as e:
        error_msg = f"Error removing duplicates: {str(e)}"
        st.error(error_msg)
        return df, error_msg


def preprocess_uploaded_file(file_bytes: bytes, filename: str, selected_sheet: str = None) -> Tuple[bool, Optional[pd.DataFrame], str]:
    """
    Main preprocessing function that handles XLSX conversion and duplicate removal.
    
    Args:
        file_bytes: File content as bytes
        filename: Original filename
        selected_sheet: Specific sheet name for Excel files (optional)
        
    Returns:
        Tuple of (success, processed_dataframe, summary_message)
    """
    try:
        # Step 1: Detect file type
        file_type = detect_file_type(file_bytes, filename)
        
        if file_type == 'unknown':
            return False, None, "❌ Unsupported file type. Please upload .xlsx or .csv files only."
        
        # Step 2: Convert XLSX to CSV if needed
        if file_type == 'xlsx':
            st.info("📄 XLSX file detected. Converting to CSV format...")
            success, df, convert_msg = convert_xlsx_to_csv(file_bytes, filename, selected_sheet)
            
            if not success:
                # Check if this is a multi-sheet scenario requiring user selection
                if convert_msg.startswith("MULTI_SHEET:"):
                    return False, None, convert_msg  # Return sheet names for selection
                else:
                    return False, None, f"❌ XLSX conversion failed: {convert_msg}"
            
            st.success(convert_msg)
            
        elif file_type == 'csv':
            st.info("📄 CSV file detected. Loading directly...")
            # Load CSV directly using pandas with error handling
            try:
                # Try different encodings if UTF-8 fails
                encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
                df = None
                
                for encoding in encodings:
                    try:
                        # Convert bytes to BytesIO for pandas
                        file_io = BytesIO(file_bytes)
                        df = pd.read_csv(file_io, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        if encoding == encodings[-1]:  # Last encoding attempt
                            raise e
                        continue
                
                if df is None:
                    return False, None, "❌ Could not decode CSV file with any supported encoding"
                
                # Basic validation
                if df.empty:
                    return False, None, "❌ CSV file is empty"
                
                if len(df.columns) == 0:
                    return False, None, "❌ CSV file has no columns"
                
                # Clean for Arrow compatibility
                df = clean_dataframe_for_arrow(df)
                
            except Exception as e:
                return False, None, f"❌ Error loading CSV file: {str(e)}"
        
        # Step 3: Remove duplicates based on Consignee Name
        st.info("🔍 Checking for duplicates based on 'Consignee Name'...")
        df_deduplicated, dedup_msg = remove_duplicates_by_consignee(df)
        
        # Step 4: Final validation
        if df_deduplicated.empty:
            return False, None, "❌ No data remaining after preprocessing"
        
        # Create summary message
        summary_parts = []
        
        if file_type == 'xlsx':
            summary_parts.append("✅ XLSX file converted to CSV format")
        
        summary_parts.append(dedup_msg)
        
        final_summary = " | ".join(summary_parts)
        
        st.success(f"🎉 Preprocessing completed successfully! Final dataset: {len(df_deduplicated)} rows × {len(df_deduplicated.columns)} columns")
        
        return True, df_deduplicated, final_summary
        
    except Exception as e:
        error_msg = f"❌ Preprocessing failed: {str(e)}"
        st.error(error_msg)
        return False, None, error_msg


def show_preprocessing_summary(original_df: pd.DataFrame, processed_df: pd.DataFrame, 
                             file_type: str, consignee_column: str = None) -> None:
    """
    Display a summary of preprocessing changes.
    
    Args:
        original_df: Original dataframe before processing
        processed_df: Processed dataframe after preprocessing
        file_type: Original file type
        consignee_column: Column used for duplicate removal
    """
    try:
        st.subheader("📊 Preprocessing Summary")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                label="File Conversion",
                value="XLSX → CSV" if file_type == 'xlsx' else "CSV (No conversion)",
                delta="✅ Converted" if file_type == 'xlsx' else "✅ Direct load"
            )
        
        with col2:
            original_rows = len(original_df) if original_df is not None else 0
            processed_rows = len(processed_df) if processed_df is not None else 0
            duplicates_removed = original_rows - processed_rows
            
            st.metric(
                label="Duplicate Removal",
                value=f"{duplicates_removed} removed",
                delta=f"From {original_rows} to {processed_rows} rows"
            )
        
        with col3:
            if consignee_column:
                st.metric(
                    label="Deduplication Column",
                    value=consignee_column,
                    delta="✅ Column found"
                )
            else:
                st.metric(
                    label="Deduplication Column",
                    value="Not found",
                    delta="⚠️ Check data"
                )
        
        # Show column information
        if processed_df is not None and not processed_df.empty:
            with st.expander("📋 Column Information"):
                st.write("**Available Columns:**")
                for i, col in enumerate(processed_df.columns, 1):
                    st.write(f"{i}. {col}")
                
                # Highlight important columns
                important_cols = []
                for col in processed_df.columns:
                    col_lower = col.lower()
                    if any(keyword in col_lower for keyword in ['consignee', 'company', 'name', 'email', 'contact']):
                        important_cols.append(col)
                
                if important_cols:
                    st.write("**Key Columns Detected:**")
                    for col in important_cols:
                        st.write(f"• {col}")
        
    except Exception as e:
        st.warning(f"Could not display preprocessing summary: {str(e)}")


def validate_preprocessed_data(df: pd.DataFrame) -> Tuple[bool, list]:
    """
    Validate the preprocessed data for common issues.
    
    Args:
        df: Preprocessed dataframe
        
    Returns:
        Tuple of (is_valid, list_of_warnings)
    """
    try:
        warnings = []
        
        if df is None or df.empty:
            return False, ["Dataset is empty"]
        
        # Check for minimum required columns
        if len(df.columns) < 2:
            warnings.append("Dataset has very few columns (less than 2)")
        
        # Check for consignee/company name column
        has_company_col = False
        company_keywords = ['consignee', 'company', 'business', 'name', 'customer', 'client']
        
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in company_keywords):
                has_company_col = True
                break
        
        if not has_company_col:
            warnings.append("No obvious company/consignee name column found")
        
        # Check for empty/null company names (WARNING ONLY, not critical)
        if has_company_col:
            for col in df.columns:
                col_lower = col.lower()
                if 'consignee' in col_lower or 'company' in col_lower:
                    null_count = df[col].isnull().sum()
                    empty_count = (df[col] == '').sum()
                    total_missing = null_count + empty_count
                    
                    if total_missing > 0:
                        warnings.append(f"Column '{col}' has {total_missing} empty/null values (not critical - processing can continue)")
                    break
        
        # Check for very small dataset
        if len(df) < 3:
            warnings.append(f"Dataset is very small ({len(df)} rows)")
        
        # Check for duplicate columns
        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        if duplicate_cols:
            warnings.append(f"Duplicate column names found: {duplicate_cols}")
        
        # UPDATED: Only critical issues that should prevent processing
        critical_issues = [w for w in warnings if any(critical in w.lower() for critical in [
            'dataset is empty',
            'no obvious company/consignee name column found', 
            'duplicate column names found'
        ])]
        
        # FIXED: Empty/null values are NOT critical - allow processing to continue
        is_valid = len(critical_issues) == 0
        
        return is_valid, warnings
        
    except Exception as e:
        return False, [f"Error validating data: {str(e)}"]
