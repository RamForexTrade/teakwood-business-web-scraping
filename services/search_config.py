"""
Search Configuration for Timber Business Research
Configure search layers and API settings
"""

# Search Layer Configuration
SEARCH_LAYERS_CONFIG = {
    'enable_general_search': True,  # Always enabled - general business search
    'enable_government_search': False,  # Government databases and official sources  
    'enable_industry_search': False,   # Timber industry specific sources
}

# API Configuration
API_CONFIG = {
    'groq_model': 'llama-3.3-70b-versatile',
    'search_delay': 2.0,  # Seconds between API calls
    'max_retries': 3,
    'timeout': 60
}

# Search Domains Configuration
PREFERRED_DOMAINS = {
    "Government": [
        "gov.in", "nic.in", "india.gov.in", "mca.gov.in", 
        "cbic.gov.in", "incometax.gov.in", "gst.gov.in",
        "moef.gov.in", "forest.gov.in", "cpcb.nic.in"
    ],
    "Industry": [
        "fidr.org", "plywoodassociation.org", "itpo.gov.in",
        "cii.in", "ficci.in", "assocham.org", "fidr.in"
    ]
}

def get_enabled_layers():
    """Get list of enabled search layers"""
    layers = []
    if SEARCH_LAYERS_CONFIG.get('enable_general_search', True):
        layers.append('General')
    if SEARCH_LAYERS_CONFIG.get('enable_government_search', False):
        layers.append('Government')
    if SEARCH_LAYERS_CONFIG.get('enable_industry_search', False):
        layers.append('Industry')
    return layers

def get_search_summary():
    """Get summary of current search configuration"""
    enabled = get_enabled_layers()
    if len(enabled) == 1:
        return f"{enabled[0]} search only"
    elif len(enabled) == 2:
        return f"{enabled[0]} + {enabled[1]} search"
    elif len(enabled) == 3:
        return "Comprehensive search (all layers)"
    else:
        return "No search layers enabled"

def get_search_config():
    """Get complete search configuration"""
    return {
        'layers': SEARCH_LAYERS_CONFIG,
        'api': API_CONFIG,
        'domains': PREFERRED_DOMAINS,
        'enabled_layers': get_enabled_layers(),
        'summary': get_search_summary()
    }
