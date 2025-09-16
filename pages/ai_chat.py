"""
AI Chat Page
============
Chat with your CSV data using AI powered by Groq API.
"""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import requests
import re
import os
from state_management import get_state
from utils.layout import render_header


# Helper function to get environment variables
def get_env_var(key, default=None):
    """Get environment variable from .env file or Streamlit secrets"""
    # First try regular environment variables
    value = os.getenv(key)
    if value:
        return value
    
    # Then try Streamlit secrets (for Streamlit Cloud deployment)
    try:
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    
    return default


def detect_identifier_columns(df):
    """Detect columns that should be treated as identifiers (like HS codes) rather than numeric values"""
    identifier_columns = []
    identifier_patterns = [
        # HS codes and trade-related identifiers
        r'.*hs.*code.*', r'.*harmonized.*', r'.*tariff.*', r'.*commodity.*code.*',
        # Product/item identifiers
        r'.*product.*code.*', r'.*item.*code.*', r'.*sku.*', r'.*barcode.*', r'.*upc.*',
        # General ID patterns
        r'.*\\bid\\b.*', r'.*identifier.*', r'.*ref.*', r'.*code.*', r'.*key.*',
        # Postal/geographic codes
        r'.*zip.*', r'.*postal.*', r'.*country.*code.*', r'.*region.*code.*',
        # Other common identifiers
        r'.*serial.*', r'.*batch.*', r'.*lot.*', r'.*consignee.*'
    ]
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Check if column name matches identifier patterns
        is_identifier_by_name = any(re.match(pattern, col_lower) for pattern in identifier_patterns)
        
        # Check data characteristics for likely identifiers
        if df[col].dtype in ['int64', 'float64'] or df[col].dtype == 'object':
            sample_values = df[col].dropna().astype(str).head(100)
            
            if len(sample_values) > 0:
                # Check for patterns that suggest identifiers
                has_leading_zeros = any(val.startswith('0') and len(val) > 1 for val in sample_values if val.isdigit())
                has_fixed_length = len(set(len(str(val)) for val in sample_values)) <= 3
                mostly_unique = df[col].nunique() / len(df) > 0.8
                contains_non_numeric = any(not str(val).replace('.', '').isdigit() for val in sample_values)
                
                # HS codes are typically 4-10 digits
                looks_like_hs_code = all(
                    len(str(val).replace('.', '')) >= 4 and
                    len(str(val).replace('.', '')) <= 10
                    for val in sample_values[:10] if str(val).replace('.', '').isdigit()
                )
                
                is_identifier_by_data = (
                    has_leading_zeros or
                    (has_fixed_length and mostly_unique) or
                    (looks_like_hs_code and col_lower in ['hs_code', 'hs', 'code', 'tariff_code']) or
                    (contains_non_numeric and not df[col].dtype in ['datetime64[ns]'])
                )
                
                if is_identifier_by_name or is_identifier_by_data:
                    identifier_columns.append(col)
    
    return identifier_columns


class CSVDataAI:
    """AI assistant for CSV data analysis using Groq"""

    def __init__(self):
        self.data_summary = {}
        self.identifier_columns = []
        self._groq_api_key = None  # Cache the API key

    @property
    def groq_api_key(self):
        """Lazy load and cache the API key to avoid repeated dotenv calls"""
        if self._groq_api_key is None:
            self._groq_api_key = get_env_var('GROQ_API_KEY')
        return self._groq_api_key
        
    def analyze_dataset(self, df, identifier_cols=None):
        """Analyze dataset structure and content"""
        if identifier_cols:
            self.identifier_columns = identifier_cols
            
        analysis = {
            "basic_info": {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "memory_usage": f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
            },
            "column_types": {},
            "sample_data": df.head(3).to_dict('records'),
            "missing_data": df.isnull().sum().to_dict(),
            "numeric_summary": {},
            "categorical_summary": {},
            "identifier_summary": {}
        }
        
        # Analyze each column
        for col in df.columns:
            dtype = str(df[col].dtype)
            analysis["column_types"][col] = dtype
            
            # Handle identifier columns specially
            if col in self.identifier_columns:
                value_counts = df[col].value_counts().head(10)
                analysis["identifier_summary"][col] = {
                    "unique_count": int(df[col].nunique()),
                    "top_values": value_counts.to_dict(),
                    "most_common": str(value_counts.index[0]) if len(value_counts) > 0 else "N/A",
                    "sample_values": df[col].dropna().head(5).tolist()
                }
            
            # Numeric columns (excluding identifiers)
            elif df[col].dtype in ['int64', 'float64', 'int32', 'float32'] and col not in self.identifier_columns:
                analysis["numeric_summary"][col] = {
                    "min": float(df[col].min()) if not df[col].empty else 0,
                    "max": float(df[col].max()) if not df[col].empty else 0,
                    "mean": float(df[col].mean()) if not df[col].empty else 0,
                    "std": float(df[col].std()) if not df[col].empty else 0,
                    "unique_count": int(df[col].nunique())
                }
            
            # Categorical/text columns (excluding identifiers)
            elif (df[col].dtype == 'object' or df[col].dtype.name == 'category') and col not in self.identifier_columns:
                value_counts = df[col].value_counts().head(10)
                analysis["categorical_summary"][col] = {
                    "unique_count": int(df[col].nunique()),
                    "top_values": value_counts.to_dict(),
                    "most_common": str(value_counts.index[0]) if len(value_counts) > 0 else "N/A"
                }
        
        self.data_summary = analysis
        return analysis
    
    def generate_data_context(self, df, question=""):
        """Generate context about the data for AI"""
        if not self.data_summary:
            self.analyze_dataset(df, self.identifier_columns)
        
        context = f"""
DATASET OVERVIEW:
- Dataset has {self.data_summary['basic_info']['rows']:,} rows and {self.data_summary['basic_info']['columns']} columns
- Memory usage: {self.data_summary['basic_info']['memory_usage']}
- Columns: {', '.join(self.data_summary['basic_info']['column_names'])}

COLUMN TYPES:"""
        
        for col, dtype in self.data_summary['column_types'].items():
            col_type = "identifier" if col in self.identifier_columns else dtype
            context += f"\n- {col}: {col_type}"
        
        # Identifier columns summary
        if self.data_summary['identifier_summary']:
            context += "\n\nIDENTIFIER COLUMNS (HS Codes, Product Codes, etc.):"
            for col, stats in self.data_summary['identifier_summary'].items():
                context += f"\n- {col}: {stats['unique_count']} unique values, most common: '{stats['most_common']}'"
                context += f"\n  Sample values: {', '.join(map(str, stats['sample_values'][:3]))}"
        
        context += "\n\nNUMERIC COLUMNS SUMMARY:"
        for col, stats in self.data_summary['numeric_summary'].items():
            context += f"\n- {col}: min={stats['min']:.2f}, max={stats['max']:.2f}, mean={stats['mean']:.2f}, unique={stats['unique_count']}"
        
        context += "\n\nCATEGORICAL COLUMNS SUMMARY:"
        for col, stats in self.data_summary['categorical_summary'].items():
            top_value = list(stats['top_values'].keys())[0] if stats['top_values'] else 'N/A'
            context += f"\n- {col}: {stats['unique_count']} unique values, most common: '{top_value}'"
        
        context += "\n\nSAMPLE DATA (first 3 rows):"
        for i, row in enumerate(self.data_summary['sample_data']):
            context += f"\nRow {i+1}: {row}"
        
        return context
    
    def get_groq_response(self, question, df):
        """Get response using Groq API"""
        if not self.groq_api_key:
            return "❌ Groq API key not found. Please add GROQ_API_KEY to your environment variables or Streamlit secrets."
        
        try:
            context = self.generate_data_context(df, question)
            
            prompt = f"""You are a data analyst. Answer questions about this dataset:

{context}

Question: {question}

Provide specific insights based on the data shown above. Note that identifier columns (like HS codes) are categorical, not numeric. Focus on actionable insights and patterns in the data."""
            
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.groq_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 1500,
                    "temperature": 0.2
                }
            )
            
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]
            else:
                return f"❌ Groq API error: {response.status_code}. Please check your API key."
        
        except Exception as e:
            return f"❌ Error: {str(e)}"
    
    def dataframe_agent(self, question: str, df: pd.DataFrame):
        """Lightweight DataFrame agent for simple operations"""
        try:
            q = question.strip().lower()
            if len(q) < 3:
                return False, ""
            
            # Simple operations
            if "how many" in q or "count" in q:
                return True, f"Total rows: {len(df):,}"
            
            if "columns" in q:
                return True, f"Columns: {', '.join(df.columns)}"
            
            if "show" in q and "data" in q:
                return True, df.head(10).to_string()
            
            return False, ""
        
        except Exception as e:
            return True, f"Error: {str(e)}"
    
    def get_ai_response(self, question, df):
        """Get AI response for the question"""
        # Try DataFrame Agent first for simple operations
        handled, agent_answer = self.dataframe_agent(question, df)
        if handled:
            return agent_answer
        
        # Use Groq for complex analysis
        return self.get_groq_response(question, df)


def render():
    """Render the AI Chat page"""
    render_header("🤖 AI Chat", "Ask questions about your data using AI-powered analysis")
    
    state = get_state()
    
    # Check if we have data
    if state.main_dataframe is None:
        st.warning("⚠️ No data available. Please upload a file first in the Upload Data page.")
        st.info("👆 Go to the Upload Data page to upload your CSV or Excel file.")
        return
    
    # Get the working dataframe - import only when needed to avoid repeated dotenv loading
    try:
        from controllers import get_display_dataframe
        df = get_display_dataframe()
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return
    
    if df is None or df.empty:
        st.error("❌ No data available for analysis.")
        return
    
    # Initialize AI assistant
    if 'csv_ai_assistant' not in st.session_state:
        st.session_state.csv_ai_assistant = CSVDataAI()
    
    ai_assistant = st.session_state.csv_ai_assistant
    
    # Detect identifier columns
    identifier_cols = detect_identifier_columns(df)
    ai_assistant.identifier_columns = identifier_cols
    
    # Show data info
    st.info(f"📊 Analyzing dataset: {len(df):,} rows × {len(df.columns)} columns")
    
    if identifier_cols:
        st.info(f"🔑 Identifier columns detected: {', '.join(identifier_cols)}")
    
    # Initialize chat history
    if 'ai_chat_history' not in st.session_state:
        st.session_state.ai_chat_history = []
    
    # Suggested questions based on data
    st.subheader("💡 Suggested Questions")
    
    columns = df.columns.tolist()
    numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns if col not in identifier_cols]
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    
    # Generate smart suggestions based on actual data
    suggestions = [
        "What's the overview of this dataset?",
        "What are the key insights from this data?",
        "Analyze the data distribution and patterns"
    ]
    
    if identifier_cols:
        suggestions.append(f"What are the most common values in {identifier_cols[0]}?")
        if any('consignee' in col.lower() for col in identifier_cols):
            suggestions.append("Analyze the consignee patterns")
    
    if numeric_cols:
        suggestions.append(f"Analyze the distribution of {numeric_cols[0]}")
        if len(numeric_cols) > 1:
            suggestions.append("What correlations exist between numeric columns?")
    
    if categorical_cols:
        suggestions.append(f"What are the top categories in {categorical_cols[0]}?")
    
    # Display suggestion buttons
    cols = st.columns(3)
    for i, suggestion in enumerate(suggestions[:6]):
        with cols[i % 3]:
            if st.button(f"📋 {suggestion[:30]}...", key=f"suggestion_{i}", help=suggestion):
                st.session_state.current_ai_question = suggestion
    
    st.divider()
    
    # Chat interface
    st.subheader("💬 Chat with Your Data")
    
    # Question input
    user_question = st.text_area(
        "🎯 Ask anything about your data:",
        value=st.session_state.get('current_ai_question', ''),
        height=100,
        placeholder="e.g., What are the top consignee names? Analyze value distributions, Find patterns in the data"
    )
    
    # Action buttons
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        ask_button = st.button("🤖 Analyze with AI", type="primary")
    
    with col2:
        if st.button("🗑️ Clear Chat"):
            st.session_state.ai_chat_history = []
            st.session_state.current_ai_question = ""
            st.rerun()
    
    with col3:
        if st.button("📊 Quick Stats"):
            st.session_state.current_ai_question = "Give me key statistics and insights about this dataset"
    
    # Process question
    if ask_button and user_question.strip():
        with st.spinner("🤔 AI is analyzing your data..."):
            try:
                response = ai_assistant.get_ai_response(user_question, df)
                
                # Add to chat history
                st.session_state.ai_chat_history.append({
                    "question": user_question,
                    "answer": response,
                    "timestamp": datetime.now().strftime("%H:%M:%S")
                })
                
                st.session_state.current_ai_question = ""
                st.rerun()
            
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # Display chat history
    if st.session_state.ai_chat_history:
        st.divider()
        st.subheader("💬 Analysis History")
        
        for chat in reversed(st.session_state.ai_chat_history[-5:]):  # Show last 5
            # User question
            with st.container():
                st.markdown(f"""
                <div style="background-color: rgba(33, 150, 243, 0.1); padding: 15px; border-radius: 10px; margin: 10px 0; border-left: 4px solid #2196f3;">
                    <strong>🙋 You ({chat['timestamp']}):</strong><br>
                    {chat['question']}
                </div>
                """, unsafe_allow_html=True)
                
                # AI response
                answer_text = chat['answer'].replace('\n', '<br>')
                st.markdown(f"""
                <div style="background-color: rgba(76, 175, 80, 0.1); padding: 15px; border-radius: 10px; margin: 10px 0; border-left: 4px solid #4caf50;">
                    <strong>🤖 AI Assistant:</strong><br>
                    {answer_text}
                </div>
                """, unsafe_allow_html=True)
    
    # Help section
    with st.expander("ℹ️ How to use AI Chat"):
        st.markdown("""
        **What you can ask:**
        - **Data overview**: "What's in this dataset?" or "Give me key insights"
        - **Column analysis**: "Analyze the distribution of [column name]"
        - **Patterns**: "What patterns do you see?" or "Find correlations"
        - **Specific questions**: "What are the top consignee names?" or "Show me value trends"
        - **Comparisons**: "Compare categories" or "What's the relationship between X and Y?"
        
        **Tips:**
        - Be specific about which columns you're interested in
        - Ask for insights, patterns, or trends
        - The AI understands your data structure automatically
        - Identifier columns (like HS codes) are treated as categories, not numbers
        """)
