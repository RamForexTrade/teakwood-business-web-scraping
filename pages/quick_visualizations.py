"""
Quick Visualizations Page
========================
Generate automatic visualizations and insights from CSV data.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
from state_management import get_state
from utils.layout import render_header


def detect_identifier_columns(df):
    """Detect columns that should be treated as identifiers"""
    identifier_columns = []
    identifier_patterns = [
        r'.*hs.*code.*', r'.*harmonized.*', r'.*tariff.*', r'.*commodity.*code.*',
        r'.*product.*code.*', r'.*item.*code.*', r'.*sku.*', r'.*barcode.*',
        r'.*\\bid\\b.*', r'.*identifier.*', r'.*ref.*', r'.*code.*', r'.*key.*',
        r'.*zip.*', r'.*postal.*', r'.*country.*code.*', r'.*region.*code.*',
        r'.*serial.*', r'.*batch.*', r'.*lot.*', r'.*consignee.*'
    ]
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Check if column name matches identifier patterns
        is_identifier_by_name = any(re.match(pattern, col_lower) for pattern in identifier_patterns)
        
        # Check data characteristics
        if df[col].dtype in ['int64', 'float64'] or df[col].dtype == 'object':
            sample_values = df[col].dropna().astype(str).head(100)
            
            if len(sample_values) > 0:
                has_leading_zeros = any(val.startswith('0') and len(val) > 1 for val in sample_values if val.isdigit())
                has_fixed_length = len(set(len(str(val)) for val in sample_values)) <= 3
                mostly_unique = df[col].nunique() / len(df) > 0.8
                
                is_identifier_by_data = has_leading_zeros or (has_fixed_length and mostly_unique)
                
                if is_identifier_by_name or is_identifier_by_data:
                    identifier_columns.append(col)
    
    return identifier_columns


def create_data_overview_viz(df, identifier_cols):
    """Create overview visualizations"""
    st.markdown("### 📊 Dataset Overview")
    
    # Basic metrics
    col1, col2, col3, col4 = st.columns(4)
    
    numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns if col not in identifier_cols]
    categorical_cols = [col for col in df.select_dtypes(include=['object']).columns if col not in identifier_cols]
    
    with col1:
        st.metric("Total Rows", f"{len(df):,}")
    
    with col2:
        st.metric("Total Columns", len(df.columns))
    
    with col3:
        st.metric("Numeric Columns", len(numeric_cols))
    
    with col4:
        st.metric("Text/ID Columns", len(categorical_cols) + len(identifier_cols))
    
    # Missing data visualization
    missing_data = df.isnull().sum()
    missing_data = missing_data[missing_data > 0].sort_values(ascending=False)
    
    if not missing_data.empty:
        st.markdown("#### 🔍 Missing Data Analysis")
        fig = px.bar(
            x=missing_data.values,
            y=missing_data.index,
            orientation='h',
            title="Missing Values by Column",
            labels={'x': 'Missing Count', 'y': 'Column'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


def create_categorical_viz(df, identifier_cols):
    """Create visualizations for categorical and identifier columns"""
    categorical_cols = [col for col in df.select_dtypes(include=['object']).columns if col not in identifier_cols]
    all_categorical = categorical_cols + identifier_cols
    
    if not all_categorical:
        st.info("No categorical or identifier columns found for visualization.")
        return
    
    st.markdown("### 📋 Categorical Data Analysis")
    
    # Column selection
    col1, col2 = st.columns(2)
    
    with col1:
        selected_col = st.selectbox(
            "Select column to analyze:",
            options=all_categorical,
            key="cat_viz_selector"
        )
    
    with col2:
        show_top_n = st.slider("Show top N values:", min_value=5, max_value=20, value=10, key="top_n_slider")
    
    if selected_col:
        # Value counts analysis
        value_counts = df[selected_col].value_counts().head(show_top_n)
        
        # Determine if this is an identifier column
        is_identifier = selected_col in identifier_cols
        col_type = "Identifier" if is_identifier else "Categorical"
        
        # Create visualization
        col1, col2 = st.columns(2)
        
        with col1:
            # Bar chart
            fig = px.bar(
                x=value_counts.values,
                y=value_counts.index,
                orientation='h',
                title=f"Top {len(value_counts)} Values in {selected_col} ({col_type})",
                labels={'x': 'Count', 'y': selected_col}
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Pie chart
            fig = px.pie(
                values=value_counts.values,
                names=value_counts.index,
                title=f"Distribution of {selected_col}"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        # Statistics
        st.markdown("#### 📈 Statistics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Unique Values", df[selected_col].nunique())
        
        with col2:
            st.metric("Most Common", str(value_counts.index[0]) if len(value_counts) > 0 else "N/A")
        
        with col3:
            uniqueness_ratio = (df[selected_col].nunique() / len(df)) * 100
            st.metric("Uniqueness %", f"{uniqueness_ratio:.1f}%")


def create_numeric_viz(df, identifier_cols):
    """Create visualizations for numeric columns"""
    numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns if col not in identifier_cols]
    
    if not numeric_cols:
        st.info("No numeric columns found for visualization.")
        return
    
    st.markdown("### 📈 Numeric Data Analysis")
    
    # Column selection
    selected_col = st.selectbox(
        "Select numeric column:",
        options=numeric_cols,
        key="num_viz_selector"
    )
    
    if selected_col:
        # Create multiple visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            # Histogram
            fig = px.histogram(
                df,
                x=selected_col,
                title=f"Distribution of {selected_col}",
                nbins=30
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Box plot
            fig = px.box(
                df,
                y=selected_col,
                title=f"Box Plot of {selected_col}"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Statistics
        st.markdown("#### 📊 Statistics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Mean", f"{df[selected_col].mean():.2f}")
        
        with col2:
            st.metric("Median", f"{df[selected_col].median():.2f}")
        
        with col3:
            st.metric("Min", f"{df[selected_col].min():.2f}")
        
        with col4:
            st.metric("Max", f"{df[selected_col].max():.2f}")


def create_correlation_viz(df, identifier_cols):
    """Create correlation visualizations"""
    numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns if col not in identifier_cols]
    
    if len(numeric_cols) < 2:
        st.info("Need at least 2 numeric columns for correlation analysis.")
        return
    
    st.markdown("### 🔥 Correlation Analysis")
    
    # Correlation matrix
    corr_matrix = df[numeric_cols].corr()
    
    # Heatmap
    fig = px.imshow(
        corr_matrix,
        title="Correlation Matrix of Numeric Columns",
        aspect="auto",
        color_continuous_scale="RdBu",
        zmin=-1, zmax=1
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)
    
    # Find strong correlations
    st.markdown("#### 🔍 Strong Correlations")
    
    # Get correlation pairs
    correlations = []
    for i in range(len(numeric_cols)):
        for j in range(i+1, len(numeric_cols)):
            corr_value = corr_matrix.iloc[i, j]
            if abs(corr_value) > 0.5:  # Strong correlation threshold
                correlations.append({
                    'Column 1': numeric_cols[i],
                    'Column 2': numeric_cols[j],
                    'Correlation': corr_value,
                    'Strength': 'Strong' if abs(corr_value) > 0.7 else 'Moderate'
                })
    
    if correlations:
        corr_df = pd.DataFrame(correlations)
        corr_df['Correlation'] = corr_df['Correlation'].round(3)
        st.dataframe(corr_df, use_container_width=True)
    else:
        st.info("No strong correlations (>0.5) found between numeric columns.")


def create_scatter_viz(df, identifier_cols):
    """Create scatter plot visualizations"""
    numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns if col not in identifier_cols]
    categorical_cols = [col for col in df.select_dtypes(include=['object']).columns if col not in identifier_cols]
    
    if len(numeric_cols) < 2:
        st.info("Need at least 2 numeric columns for scatter plots.")
        return
    
    st.markdown("### 📍 Relationship Analysis")
    
    # Column selection
    col1, col2, col3 = st.columns(3)
    
    with col1:
        x_col = st.selectbox("X-axis (numeric):", options=numeric_cols, key="scatter_x")
    
    with col2:
        y_col = st.selectbox("Y-axis (numeric):", options=numeric_cols, key="scatter_y", index=1 if len(numeric_cols) > 1 else 0)
    
    with col3:
        color_col = st.selectbox("Color by (optional):", options=["None"] + categorical_cols, key="scatter_color")
    
    if x_col and y_col and x_col != y_col:
        # Create scatter plot
        if color_col != "None":
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                color=color_col,
                title=f"{y_col} vs {x_col} (colored by {color_col})",
                hover_data=[color_col]
            )
        else:
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                title=f"{y_col} vs {x_col}"
            )
        
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Show correlation
        correlation = df[x_col].corr(df[y_col])
        st.metric("Correlation Coefficient", f"{correlation:.3f}")


def create_summary_insights(df, identifier_cols):
    """Create automatic insights summary"""
    st.markdown("### 🎯 Automatic Insights")
    
    insights = []
    
    # Data size insight
    insights.append(f"📊 Dataset contains {len(df):,} rows and {len(df.columns)} columns")
    
    # Missing data insight
    missing_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
    if missing_percentage > 10:
        insights.append(f"⚠️ Dataset has {missing_percentage:.1f}% missing values - consider data cleaning")
    elif missing_percentage > 0:
        insights.append(f"✅ Dataset has minimal missing values ({missing_percentage:.1f}%)")
    else:
        insights.append("✅ No missing values detected")
    
    # Column type insights
    numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns if col not in identifier_cols]
    categorical_cols = [col for col in df.select_dtypes(include=['object']).columns if col not in identifier_cols]
    
    if identifier_cols:
        insights.append(f"🔑 {len(identifier_cols)} identifier column(s) detected: {', '.join(identifier_cols[:3])}")
    
    if numeric_cols:
        insights.append(f"📈 {len(numeric_cols)} numeric column(s) available for statistical analysis")
    
    if categorical_cols:
        insights.append(f"📋 {len(categorical_cols)} categorical column(s) for grouping and filtering")
    
    # Data distribution insights
    for col in numeric_cols[:3]:  # Analyze first 3 numeric columns
        skewness = df[col].skew()
        if abs(skewness) > 1:
            direction = "right" if skewness > 0 else "left"
            insights.append(f"📊 Column '{col}' is highly skewed to the {direction}")
    
    # Uniqueness insights
    for col in df.columns[:5]:  # Check first 5 columns
        uniqueness_ratio = df[col].nunique() / len(df)
        if uniqueness_ratio > 0.95:
            insights.append(f"🔑 Column '{col}' has very high uniqueness (potential identifier)")
        elif uniqueness_ratio < 0.05:
            insights.append(f"📊 Column '{col}' has low variety (only {df[col].nunique()} unique values)")
    
    # Display insights
    for insight in insights:
        st.markdown(f"• {insight}")


def render():
    """Render the Quick Visualizations page"""
    render_header("📈 Quick Visualizations", "Automatic insights and visualizations from your data")
    
    state = get_state()
    
    # Check if we have data
    if state.main_dataframe is None:
        st.warning("⚠️ No data available. Please upload a file first in the Upload Data page.")
        st.info("👆 Go to the Upload Data page to upload your CSV or Excel file.")
        return
    
    # Get the working dataframe
    from controllers import get_display_dataframe
    df = get_display_dataframe()
    
    if df is None or df.empty:
        st.error("❌ No data available for visualization.")
        return
    
    # Detect identifier columns
    identifier_cols = detect_identifier_columns(df)
    
    # Show data info
    st.info(f"📊 Visualizing dataset: {len(df):,} rows × {len(df.columns)} columns")
    
    if identifier_cols:
        st.info(f"🔑 Identifier columns detected: {', '.join(identifier_cols)}")
    
    # Create visualization sections
    create_data_overview_viz(df, identifier_cols)
    
    st.divider()
    
    # Create tabs for different visualization types
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Categorical",
        "📈 Numeric", 
        "🔥 Correlations",
        "📍 Relationships",
        "🎯 Insights"
    ])
    
    with tab1:
        create_categorical_viz(df, identifier_cols)
    
    with tab2:
        create_numeric_viz(df, identifier_cols)
    
    with tab3:
        create_correlation_viz(df, identifier_cols)
    
    with tab4:
        create_scatter_viz(df, identifier_cols)
    
    with tab5:
        create_summary_insights(df, identifier_cols)
    
    # Download section
    st.divider()
    st.markdown("### 💾 Export Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Download Summary Report", type="secondary"):
            # Create a simple summary report
            summary_text = f"""
# Data Analysis Summary Report

## Dataset Overview
- **Rows**: {len(df):,}
- **Columns**: {len(df.columns)}
- **Identifier Columns**: {len(identifier_cols)}
- **Numeric Columns**: {len([col for col in df.select_dtypes(include=[np.number]).columns if col not in identifier_cols])}

## Column Details
"""
            for col in df.columns:
                col_type = "Identifier" if col in identifier_cols else str(df[col].dtype)
                unique_count = df[col].nunique()
                summary_text += f"- **{col}**: {col_type} ({unique_count} unique values)\n"
            
            st.download_button(
                label="📥 Download Report",
                data=summary_text,
                file_name=f"data_summary_{len(df)}rows.md",
                mime="text/markdown"
            )
    
    with col2:
        if st.button("📈 Export Visualizations Info", type="secondary"):
            st.info("💡 Use your browser's screenshot feature to save individual charts, or the download buttons on each Plotly chart.")
    
    # Help section
    with st.expander("ℹ️ How to use Quick Visualizations"):
        st.markdown("""
        **What you'll see:**
        - **📊 Overview**: Basic statistics and missing data analysis
        - **📋 Categorical**: Charts for text and identifier columns (like company names, HS codes)
        - **📈 Numeric**: Histograms and box plots for numerical data
        - **🔥 Correlations**: Relationships between numeric columns
        - **📍 Relationships**: Scatter plots to explore connections
        - **🎯 Insights**: Automatic observations about your data
        
        **Tips:**
        - Identifier columns (like HS codes) are automatically detected and treated as categories
        - Use the dropdown menus to explore different columns
        - Hover over charts for more details
        - Click and drag to zoom into specific areas
        """)
