"""
Winwood Enterprise Custom Styling
=================================
Custom CSS styling based on company branding colors and design.
"""

# Winwood Enterprise color palette based on logo
WINWOOD_COLORS = {
    'primary_blue': '#2B5F8C',      # Deep blue from logo circle
    'secondary_blue': '#4A7BA7',    # Lighter blue
    'gold': '#C8860D',              # Gold from company name
    'light_gold': '#E6A832',        # Lighter gold accent
    'white': '#FFFFFF',
    'light_gray': '#F0F2F6',
    'dark_gray': '#262730'
}

def get_winwood_css():
    """Return custom CSS styling for Winwood Enterprise branding"""
    return f"""
    <style>
    /* Main app styling */
    .stApp {{
        background: linear-gradient(135deg, {WINWOOD_COLORS['light_gray']} 0%, {WINWOOD_COLORS['white']} 100%);
    }}
    
    /* Sidebar styling */
    .css-1d391kg {{
        background: linear-gradient(180deg, {WINWOOD_COLORS['primary_blue']} 0%, {WINWOOD_COLORS['secondary_blue']} 100%);
        color: white;
    }}
    
    /* Header styling */
    .winwood-header {{
        background: linear-gradient(90deg, {WINWOOD_COLORS['primary_blue']} 0%, {WINWOOD_COLORS['secondary_blue']} 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 1rem;
    }}
    
    /* Button styling */
    .stButton > button {{
        background: linear-gradient(45deg, {WINWOOD_COLORS['gold']} 0%, {WINWOOD_COLORS['light_gold']} 100%);
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: bold;
        transition: all 0.3s ease;
    }}
    
    .stButton > button:hover {{
        background: linear-gradient(45deg, {WINWOOD_COLORS['light_gold']} 0%, {WINWOOD_COLORS['gold']} 100%);
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }}
    
    /* Primary button styling */
    .stButton > button[kind="primary"] {{
        background: linear-gradient(45deg, {WINWOOD_COLORS['primary_blue']} 0%, {WINWOOD_COLORS['secondary_blue']} 100%);
    }}
    
    .stButton > button[kind="primary"]:hover {{
        background: linear-gradient(45deg, {WINWOOD_COLORS['secondary_blue']} 0%, {WINWOOD_COLORS['primary_blue']} 100%);
    }}
    
    /* Metric styling */
    .metric-container {{
        background: white;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid {WINWOOD_COLORS['gold']};
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }}
    
    /* Success messages */
    .stSuccess {{
        background: linear-gradient(90deg, #10B981 0%, #34D399 100%);
        border-left: 4px solid #059669;
    }}
    
    /* Info messages */
    .stInfo {{
        background: linear-gradient(90deg, {WINWOOD_COLORS['primary_blue']} 0%, {WINWOOD_COLORS['secondary_blue']} 100%);
        border-left: 4px solid {WINWOOD_COLORS['primary_blue']};
        color: white;
    }}
    
    /* Warning messages */
    .stWarning {{
        background: linear-gradient(90deg, {WINWOOD_COLORS['gold']} 0%, {WINWOOD_COLORS['light_gold']} 100%);
        border-left: 4px solid {WINWOOD_COLORS['gold']};
    }}
    
    /* Chat messages */
    .user-message {{
        background: linear-gradient(135deg, {WINWOOD_COLORS['primary_blue']}20 0%, {WINWOOD_COLORS['secondary_blue']}20 100%);
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
        border-left: 4px solid {WINWOOD_COLORS['primary_blue']};
    }}
    
    .ai-message {{
        background: linear-gradient(135deg, {WINWOOD_COLORS['gold']}20 0%, {WINWOOD_COLORS['light_gold']}20 100%);
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
        border-left: 4px solid {WINWOOD_COLORS['gold']};
    }}
    
    /* Company logo styling */
    .winwood-logo {{
        filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1));
        border-radius: 50%;
    }}
    
    /* Navigation buttons */
    .nav-button {{
        background: rgba(255,255,255,0.1);
        border: 1px solid rgba(255,255,255,0.2);
        color: white;
        transition: all 0.3s ease;
    }}
    
    .nav-button:hover {{
        background: rgba(255,255,255,0.2);
        border: 1px solid {WINWOOD_COLORS['gold']};
    }}
    
    /* Footer */
    .winwood-footer {{
        text-align: center;
        padding: 1rem;
        color: {WINWOOD_COLORS['dark_gray']};
        font-size: 0.8rem;
        border-top: 1px solid {WINWOOD_COLORS['light_gray']};
        margin-top: 2rem;
    }}
    </style>
    """

def apply_winwood_styling():
    """Apply Winwood Enterprise custom styling to the page"""
    import streamlit as st
    st.markdown(get_winwood_css(), unsafe_allow_html=True)

def render_winwood_footer():
    """Render company footer"""
    import streamlit as st
    st.markdown("""
    <div class="winwood-footer">
        <strong>Winwood Enterprise Sdn Bhd</strong><br>
        Premium Timber & Wood Products | www.winwood.com.my<br>
        Advanced Business Research & Data Analysis Platform
    </div>
    """, unsafe_allow_html=True)
