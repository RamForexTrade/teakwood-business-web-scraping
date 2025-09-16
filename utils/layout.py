"""
Layout Utilities
================
UI layout helpers for consistent design across the app.
"""
import streamlit as st
from state_management import get_state, update_state
from utils.winwood_styling import apply_winwood_styling, render_winwood_footer


def setup_page_config() -> None:
    """Configure the Streamlit page settings."""
    st.set_page_config(
        page_title="Winwood Enterprise - Business Research Tool",
        page_icon="🌳",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    # Apply custom Winwood Enterprise styling
    apply_winwood_styling()


def render_header(title: str, subtitle: str = "") -> None:
    """Render consistent page header with company branding."""
    # Company header with logo and title
    col1, col2 = st.columns([1, 4])
    
    with col1:
        try:
            import os
            logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "winwood_logo.png")
            if os.path.exists(logo_path):
                st.image(logo_path, width=120)
            else:
                st.markdown("### 🌳")
        except Exception:
            st.markdown("### 🌳")
    
    with col2:
        st.markdown(f"### Winwood Enterprise Sdn Bhd")
        st.markdown(f"# {title}")
        if subtitle:
            st.markdown(f"*{subtitle}*")
    
    st.divider()


def render_navigation_sidebar() -> None:
    """Render navigation sidebar with stage indicators."""
    state = get_state()

    with st.sidebar:
        # Company header with logo
        try:
            # Try to display the company logo
            import os
            logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "winwood_logo.png")
            if os.path.exists(logo_path):
                st.image(logo_path, width=200)
            else:
                st.header("🌳 Winwood Enterprise")
        except Exception:
            st.header("🌳 Winwood Enterprise")
        
        st.subheader("Business Research Tool")
        st.caption("✨ Advanced Data Analysis & Research Platform")
        
        # Show current step
        st.subheader("Current Step")
        stage_emoji = {
            "upload": "📤",
            "map": "🔍",  # Changed from map to research icon
            "analyze": "📧",
            "ai_chat": "🤖",
            "visualizations": "📈"
        }
        
        stage_names = {
            "upload": "Upload & Filter",
            "map": "Business Research", 
            "analyze": "Email Outreach",
            "ai_chat": "AI Chat",
            "visualizations": "Quick Visualizations"
        }
        
        current_emoji = stage_emoji.get(state.current_stage, "❓")
        current_name = stage_names.get(state.current_stage, state.current_stage.title())
        st.info(f"{current_emoji} {current_name}")
        
        # Navigation buttons
        st.subheader("Navigation")
        
        # Core workflow
        if st.button("📤 Upload", use_container_width=True):
            from controllers import go_to_stage
            go_to_stage("upload")
        
        # AI Analysis Tools (new section)
        st.markdown("**🤖 AI Analysis Tools**")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🤖 AI Chat", use_container_width=True, 
                        disabled=not _can_access_ai_tools(),
                        help="Chat with your data using AI"):
                from controllers import go_to_stage
                go_to_stage("ai_chat")
        
        with col2:
            if st.button("📈 Quick Viz", use_container_width=True,
                        disabled=not _can_access_ai_tools(),
                        help="Generate automatic visualizations"):
                from controllers import go_to_stage
                go_to_stage("visualizations")
        
        # Business workflow
        st.markdown("**🔍 Business Workflow**")
        if st.button("🔍 Business Research", use_container_width=True, 
                    disabled=not _can_access_map()):
            from controllers import go_to_stage
            go_to_stage("map")
            
        if st.button("📧 Email Outreach", use_container_width=True,
                    disabled=not _can_access_analyze()):
            from controllers import go_to_stage
            go_to_stage("analyze")
        
        # Settings
        st.divider()
        st.subheader("Settings")
        
        # Clean debug mode
        debug_enabled = st.checkbox("🐛 Debug Mode", value=state.show_debug)
        update_state(show_debug=debug_enabled)
        
        if debug_enabled:
            with st.expander("📊 System Status", expanded=False):
                # Clean, essential debug info only
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Status", "Active")
                    if state.main_dataframe is not None:
                        st.metric("Rows", f"{len(state.main_dataframe):,}")
                with col2:
                    st.metric("Stage", state.current_stage.title())
                    if state.main_dataframe is not None:
                        st.metric("Columns", len(state.main_dataframe.columns))
                
                # Session info (shortened)
                st.caption(f"Session: {state.session_id[:8]}...")
                
                # Memory usage info
                if state.main_dataframe is not None:
                    memory_mb = state.main_dataframe.memory_usage(deep=True).sum() / 1024**2
                    st.caption(f"Memory: {memory_mb:.1f} MB")
        
        # Reset button
        st.divider()
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Reset App", use_container_width=True):
                from controllers import reset_app
                reset_app()
        
        with col2:
            if st.button("🧹 Clear Cache", use_container_width=True,
                        help="Clear cache and restart to refresh sidebar"):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.success("✅ Cache cleared!")
                st.info("🔄 Please restart the app to see clean sidebar")
                st.rerun()


def render_progress_indicator() -> None:
    """Render progress indicator showing current stage."""
    state = get_state()
    
    # Core workflow stages for progress calculation
    core_stages = ["upload", "map", "analyze"]
    stage_labels = {"upload": "Upload Data", "map": "Business Research", "analyze": "Email Outreach"}
    
    # Check if current stage is part of core workflow
    if state.current_stage in core_stages:
        current_index = core_stages.index(state.current_stage)
        progress_value = (current_index + 1) / len(core_stages)
        stage_name = stage_labels.get(state.current_stage, state.current_stage.title())
        st.progress(progress_value, text=f"Step {current_index + 1} of {len(core_stages)}: {stage_name}")
    
    # For AI tools, show different indicator
    elif state.current_stage in ["ai_chat", "visualizations"]:
        ai_stage_labels = {"ai_chat": "AI Chat Analysis", "visualizations": "Data Visualizations"}
        stage_name = ai_stage_labels.get(state.current_stage, state.current_stage.title())
        st.info(f"🤖 AI Analysis: {stage_name}")
    
    else:
        st.progress(0.0, text="Getting started...")


def render_file_info() -> None:
    """Render information about uploaded file."""
    state = get_state()
    
    if state.uploaded_filename:
        with st.container():
            col1, col2 = st.columns([3, 1])
            with col1:
                st.success(f"📄 Loaded: **{state.uploaded_filename}**")
            with col2:
                if state.main_dataframe is not None:
                    rows, cols = state.main_dataframe.shape
                    st.metric("Shape", f"{rows}×{cols}")


def render_error_boundary(error_message: str) -> None:
    """Render error message with consistent styling."""
    st.error(f"❌ {error_message}")


def render_success_message(message: str) -> None:
    """Render success message with consistent styling."""
    st.success(f"✅ {message}")


def render_info_message(message: str) -> None:
    """Render info message with consistent styling."""
    st.info(f"ℹ️ {message}")


def _can_access_ai_tools() -> bool:
    """Check if AI tools (chat and visualizations) are accessible."""
    state = get_state()
    return state.main_dataframe is not None


def _can_access_map() -> bool:
    """Check if map stage is accessible."""
    from controllers import can_proceed_to_map
    return can_proceed_to_map()


def _can_access_analyze() -> bool:
    """Check if analyze stage is accessible."""
    from controllers import can_proceed_to_analyze
    return can_proceed_to_analyze()


def render_filter_controls() -> None:
    """Render filter controls with ALL columns + FIXED text search for secondary filter."""
    from controllers import (
        get_filterable_columns, get_column_unique_values, 
        apply_filters, reset_filters
    )
    from state_management import get_state, update_state
    
    def validate_filter_column(df, column_name):
        """Validate if a column name is valid for filtering"""
        if not column_name or column_name.strip() == '':
            return False
        if column_name not in df.columns:
            return False
        return True
    
    state = get_state()
    
    if state.main_dataframe is None:
        return
    
    # Validate and clean state filter columns to prevent KeyError
    if hasattr(state, 'secondary_filter_column') and not validate_filter_column(state.main_dataframe, state.secondary_filter_column):
        update_state(secondary_filter_column='', secondary_filter_values=[])
    
    if hasattr(state, 'primary_filter_column') and not validate_filter_column(state.main_dataframe, state.primary_filter_column):
        update_state(primary_filter_column='', primary_filter_values=[])
    
    # Add custom CSS to make selectboxes wider and handle text overflow properly
    st.markdown("""
    <style>
    .stSelectbox > div > div > div {
        min-width: 320px !important;
        max-width: 500px !important;
    }
    .stSelectbox > div > div > div > div {
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }
    .stMultiSelect > div > div > div {
        min-width: 320px !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.subheader("🔍 Data Filters")
    st.info("ℹ️ Filter your data by any column. Text search available for large datasets!")
    
    # Get ALL filterable columns (no restrictions)
    filterable_cols = get_filterable_columns()
    
    if not filterable_cols:
        st.error("❌ No columns found for filtering. Check your data.")
        return
    
    # Show column statistics
    with st.expander("📊 Column Statistics", expanded=False):
        st.write(f"**{len(filterable_cols)} columns in your data:**")
        
        col_stats = []
        for col in filterable_cols:
            unique_count = len(get_column_unique_values(col))
            col_stats.append((col, unique_count))
        
        # Sort by unique count for better organization
        col_stats.sort(key=lambda x: x[1])
        
        for col, count in col_stats:
            if count <= 10:
                st.write(f"🟢 **{col}**: {count} values (very fast)")
            elif count <= 50:
                st.write(f"🟡 **{col}**: {count} values (fast)")
            elif count <= 200:
                st.write(f"🟠 **{col}**: {count} values (moderate)")
            else:
                st.write(f"🔴 **{col}**: {count} values (text search available)")
    
    # Enhanced layout with proper spacing
    col1, col2, col3 = st.columns([5, 5, 2])
    
    with col1:
        # Primary filter section
        st.write("**🎯 Primary Filter**")
        
        # Column selection - use empty string as placeholder
        primary_options = [""] + filterable_cols
        
        # Find current selection index
        current_primary_index = 0
        if state.primary_filter_column in primary_options:
            current_primary_index = primary_options.index(state.primary_filter_column)
        
        selected_primary = st.selectbox(
            "Choose any column to filter by:",
            options=primary_options,
            index=current_primary_index,
            key="primary_filter_column_select",
            help="Choose any column from your data to filter by",
            format_func=lambda x: "Select a column..." if x == "" else x
        )
        
        # Value selection for primary filter
        primary_values = []
        if selected_primary:
            available_values = get_column_unique_values(selected_primary)
            if available_values:
                # Show value count - just informational, no warnings
                value_count = len(available_values)
                st.caption(f"📊 {value_count:,} unique values available")
                
                if value_count > 1000:
                    st.info(f"ℹ️ Large dataset: {value_count:,} values. Use search or scroll to find values.")
                
                primary_values = st.multiselect(
                    f"Select values from **{selected_primary}**:",
                    options=available_values,
                    default=state.primary_filter_values if state.primary_filter_column == selected_primary else [],
                    key="primary_filter_values_select",
                    help=f"Choose any values from {selected_primary}. Start typing to search."
                )
                
                if primary_values:
                    st.success(f"✓ {len(primary_values)} value(s) selected")
            else:
                st.warning("No values available for this column")
        else:
            st.info("👆 Select any column above to see its values")
    
    with col2:
        # Secondary filter section with FIXED TEXT SEARCH functionality
        st.write("**🎯 Secondary Filter (Text Search)**")
        
        # Get remaining columns for secondary filter
        remaining_cols = [col for col in filterable_cols if col != selected_primary]
        secondary_options = [""] + remaining_cols
        
        # Find current selection index for secondary
        current_secondary_index = 0
        if state.secondary_filter_column and state.secondary_filter_column in remaining_cols:
            current_secondary_index = secondary_options.index(state.secondary_filter_column)
        
        selected_secondary = st.selectbox(
            "Choose second column to filter by:",
            options=secondary_options,
            index=current_secondary_index,
            key="secondary_filter_column_select",
            help="Optional: Add a second filter with smart text search",
            disabled=not selected_primary,
            format_func=lambda x: "Select a column..." if x == "" else x
        )
        
        # Value selection for secondary filter with TEXT SEARCH
        secondary_values = []
        if selected_secondary:
            available_values = get_column_unique_values(selected_secondary)
            if available_values:
                # Show value count
                value_count = len(available_values)
                st.caption(f"📊 {value_count:,} unique values available")
                
                # For columns with many values, offer text search filter
                if value_count > 100:
                    st.success(f"🔍 **Text Search Mode** - No need to select from {value_count:,} values!")
                    
                    # TEXT INPUT MODE - No checkbox, direct text input
                    search_text = st.text_input(
                        f"🔍 Search keywords in **{selected_secondary}**:",
                        key="secondary_search_text",
                        placeholder="e.g., teak, wood, pine (separate with commas)",
                        help=f"Enter keywords to search within {selected_secondary} values (case-insensitive)"
                    )
                    
                    if search_text:
                        # Filter values based on search text
                        search_terms = [term.strip().lower() for term in search_text.split(',') if term.strip()]
                        matching_values = []
                        
                        for val in available_values:
                            val_lower = str(val).lower()
                            # Check if any search term is found in the value
                            if any(term in val_lower for term in search_terms):
                                matching_values.append(val)
                        
                        if matching_values:
                            st.success(f"✅ Found **{len(matching_values)}** values containing: **{search_text}**")
                            
                            # Show preview of matching values
                            with st.expander(f"👀 Preview {len(matching_values)} matching values"):
                                for i, val in enumerate(matching_values[:15], 1):
                                    st.write(f"{i}. {val}")
                                if len(matching_values) > 15:
                                    st.write(f"... and {len(matching_values) - 15} more")
                            
                            # Use all matching values as filter
                            secondary_values = matching_values
                            
                            # Show selection summary
                            st.info(f"🎯 **Active Filter:** {len(matching_values)} matches for '{search_text}'")
                        else:
                            st.warning(f"❌ No values found containing '{search_text}'")
                            st.info("💡 Try: teak, wood, pine, lumber, timber, board, furniture")
                    else:
                        st.info("💡 Type keywords above to search")
                        st.write("**Examples:** teak, wood, pine, lumber, timber")
                        
                        # Option to switch to selection mode
                        if st.checkbox("📋 Switch to selection mode instead", key="switch_to_select"):
                            secondary_values = st.multiselect(
                                f"Select values from **{selected_secondary}**:",
                                options=available_values,
                                default=state.secondary_filter_values if state.secondary_filter_column == selected_secondary else [],
                                key="secondary_filter_values_select_fallback",
                                help=f"Choose specific values from {selected_secondary}."
                            )
                            
                            if secondary_values:
                                st.success(f"✓ {len(secondary_values)} value(s) selected")
                
                else:
                    # Regular multiselect for smaller datasets (≤100 values)
                    st.write("**📋 Select values:**")
                    secondary_values = st.multiselect(
                        f"Select values from **{selected_secondary}**:",
                        options=available_values,
                        default=state.secondary_filter_values if state.secondary_filter_column == selected_secondary else [],
                        key="secondary_filter_values_select",
                        help=f"Choose any values from {selected_secondary}. Start typing to search."
                    )
                    
                    if secondary_values:
                        st.success(f"✓ {len(secondary_values)} value(s) selected")
            else:
                st.warning("No values available for this column")
        elif not selected_primary:
            st.info("👆 Select primary filter first")
        else:
            st.info("👆 Select a column above for text search")
    
    with col3:
        # Actions and summary
        st.write("**⚡ Actions**")
        
        # Apply filters button
        apply_disabled = not (primary_values or secondary_values)
        if st.button("🔍 Apply Filters", 
                    use_container_width=True, 
                    type="primary",
                    disabled=apply_disabled,
                    help="Apply the selected filters to your data"):
            # Update state with new filter values
            update_state(
                primary_filter_column=selected_primary,
                primary_filter_values=primary_values,
                secondary_filter_column=selected_secondary,
                secondary_filter_values=secondary_values
            )
            apply_filters()
            st.rerun()
        
        # Reset filters button
        reset_disabled = not (state.primary_filter_values or state.secondary_filter_values)
        if st.button("🚮 Clear All", 
                    use_container_width=True,
                    disabled=reset_disabled,
                    help="Remove all applied filters"):
            reset_filters()
            st.rerun()
        
        # Show filter statistics
        st.divider()
        if state.main_dataframe is not None:
            total_cols = len(state.main_dataframe.columns)
            filterable_count = len(filterable_cols)
            st.metric("Available", f"{filterable_count}/{total_cols}")
            
            total_rows = len(state.main_dataframe)
            st.metric("Total Rows", f"{total_rows:,}")
    
    # Enhanced active filters display
    if state.primary_filter_values or secondary_values:
        st.divider()
        st.subheader("🎯 Active Filters")
        
        # Show filter results summary
        if state.main_dataframe is not None:
            # Apply filters to get filtered count
            filtered_df = state.main_dataframe.copy()
            
            if state.primary_filter_values and state.primary_filter_column and state.primary_filter_column in filtered_df.columns:
                mask = filtered_df[state.primary_filter_column].astype(str).isin([str(v) for v in state.primary_filter_values])
                filtered_df = filtered_df[mask]
            
            if secondary_values and state.secondary_filter_column and state.secondary_filter_column in filtered_df.columns:
                mask = filtered_df[state.secondary_filter_column].astype(str).isin([str(v) for v in secondary_values])
                filtered_df = filtered_df[mask]
            
            filtered_count = len(filtered_df)
            total_count = len(state.main_dataframe)
            
            st.info(f"📊 **Showing {filtered_count:,} of {total_count:,} rows** ({filtered_count/total_count*100:.1f}%)")
        
        # Create columns for filter display
        if state.primary_filter_values and secondary_values:
            filter_cols = st.columns(2)
        else:
            filter_cols = [st.container()]
        
        filter_col_idx = 0
        
        if state.primary_filter_values:
            with filter_cols[filter_col_idx] if len(filter_cols) > 1 else filter_cols[0]:
                st.success("✅ **Primary Filter Active**")
                st.write(f"**Column:** {state.primary_filter_column}")
                st.write(f"**Selected:** {len(state.primary_filter_values)} value(s)")
                
                # Show selected values in an expander
                with st.expander(f"View {len(state.primary_filter_values)} selected values"):
                    for i, value in enumerate(state.primary_filter_values, 1):
                        st.write(f"{i}. {value}")
            filter_col_idx = 1
        
        if secondary_values:
            with filter_cols[filter_col_idx] if len(filter_cols) > 1 else filter_cols[0]:
                st.success("✅ **Secondary Filter Active (Text Search)**")
                st.write(f"**Column:** {selected_secondary}")
                st.write(f"**Matched:** {len(secondary_values)} value(s)")
                
                # Show matched values in an expander
                with st.expander(f"View {len(secondary_values)} matched values"):
                    for i, value in enumerate(secondary_values[:50], 1):  # Limit display
                        st.write(f"{i}. {value}")
                    if len(secondary_values) > 50:
                        st.write(f"... and {len(secondary_values) - 50} more")
    
    # Pro tips for text search
    with st.expander("💡 Text Search Examples"):
        st.write("""
        **Search Examples for Product Description:**
        
        🔍 **Single keyword:**
        - Type `teak` → finds "TEAK WOOD LOGS", "Teak lumber", etc.
        - Type `pine` → finds "RED WOOD PINE", "Pine lumber", etc.
        
        🔍 **Multiple keywords (OR search):**
        - Type `teak, wood` → finds products with 'teak' OR 'wood'
        - Type `lumber, timber` → finds products with 'lumber' OR 'timber'
        
        🔍 **More examples:**
        - `furniture` → finds furniture items
        - `board, plywood` → finds boards or plywood
        - `rough, sawn` → finds rough or sawn timber
        
        ✨ **Case insensitive:** TEAK, teak, Teak all work the same!
        """)


def render_data_preview(df, max_rows: int = 5) -> None:
    """Render data preview with consistent styling."""
    from utils.data_utils import safe_dataframe_display
    
    if df is not None:
        st.subheader("📊 Data Preview")
        
        # Show basic info
        rows, cols = df.shape
        st.write(f"**Shape:** {rows} rows × {cols} columns")
        
        # Show column info
        with st.expander("Column Information"):
            try:
                col_info = df.dtypes.reset_index()
                col_info.columns = ['Column', 'Type']
                # Clean column info for display
                col_info = safe_dataframe_display(col_info, max_rows=len(col_info))
                st.dataframe(col_info, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not display column info: {str(e)}")
        
        # Show data preview with safe display
        try:
            display_df = safe_dataframe_display(df, max_rows)
            st.dataframe(display_df, use_container_width=True)
            
            if len(df) > max_rows:
                st.caption(f"Showing first {max_rows} rows of {len(df)} total rows")
        except Exception as e:
            st.error(f"Error displaying data preview: {str(e)}")
            st.info("Data contains incompatible types. Try cleaning the data first.")
