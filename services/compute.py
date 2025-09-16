"""
Enhanced Compute Service with Web Research Integration
Teakwood Business Web Scraping - Stage 4 Implementation
"""

import pandas as pd
import streamlit as st
from typing import Dict, List, Optional, Callable
import time
from datetime import datetime
import json

from utils.data_utils import clean_dataframe_for_arrow
from services.web_scraper import WebScraper, ResearchResultsManager, perform_dummy_web_search


class WebResearchOrchestrator:
    """
    Orchestrates web research operations for business contact discovery.
    Integrates with existing compute.py structure.
    """
    
    def __init__(self):
        self.scraper = WebScraper()
        self.results_manager = ResearchResultsManager()
    
    def perform_web_research(self, company_names: List[str], 
                           progress_callback: Optional[Callable] = None) -> Dict:
        """
        Orchestrate web research for multiple companies.
        
        Args:
            company_names (List[str]): List of company names to research
            progress_callback (Callable): Optional progress callback
            
        Returns:
            Dict: Complete research results with metadata
        """
        start_time = datetime.now()
        
        # Initialize research session
        research_session = {
            'session_id': st.session_state.get('session_id', 'unknown'),
            'start_time': start_time.isoformat(),
            'total_companies': len(company_names),
            'status': 'in_progress'
        }
        
        try:
            # Perform batch research
            results = self.scraper.batch_research_with_progress(
                company_names, 
                progress_callback
            )
            
            research_session.update({
                'status': 'completed',
                'end_time': datetime.now().isoformat(),
                'results': results,
                'success_count': len([r for r in results.values() if r['status'] == 'found']),
                'failure_count': len([r for r in results.values() if r['status'] != 'found'])
            })
            
        except Exception as e:
            research_session.update({
                'status': 'failed',
                'end_time': datetime.now().isoformat(),
                'error': str(e),
                'results': {}
            })
            
        return research_session
    
    def analyze_research_results(self, research_data: Dict) -> Dict:
        """
        Analyze and score research results for quality assessment.
        
        Args:
            research_data (Dict): Raw research results
            
        Returns:
            Dict: Analysis summary with quality metrics
        """
        results = research_data.get('results', {})
        
        analysis = {
            'total_researched': len(results),
            'successful_research': 0,
            'failed_research': 0,
            'average_confidence': 0.0,
            'quality_distribution': {'high': 0, 'medium': 0, 'low': 0},
            'contact_types_found': {},
            'data_completeness': {
                'has_email': 0,
                'has_phone': 0,
                'has_website': 0,
                'has_social': 0
            }
        }
        
        if not results:
            return analysis
        
        confidence_scores = []
        
        for company_name, result in results.items():
            if result['status'] == 'found':
                analysis['successful_research'] += 1
                confidence = result.get('confidence_score', 0.0)
                confidence_scores.append(confidence)
                
                # Quality categorization
                if confidence >= 0.8:
                    analysis['quality_distribution']['high'] += 1
                elif confidence >= 0.6:
                    analysis['quality_distribution']['medium'] += 1
                else:
                    analysis['quality_distribution']['low'] += 1
                
                # Analyze contact types
                contacts = result.get('contacts', [])
                for contact in contacts:
                    contact_type = contact.get('type', 'unknown')
                    analysis['contact_types_found'][contact_type] = \
                        analysis['contact_types_found'].get(contact_type, 0) + 1
                
                # Data completeness analysis
                if contacts:
                    analysis['data_completeness']['has_email'] += 1
                    if any(c.get('phone') for c in contacts):
                        analysis['data_completeness']['has_phone'] += 1
                
                if result.get('website'):
                    analysis['data_completeness']['has_website'] += 1
                    
                if result.get('social_media'):
                    analysis['data_completeness']['has_social'] += 1
                    
            else:
                analysis['failed_research'] += 1
        
        # Calculate averages
        if confidence_scores:
            analysis['average_confidence'] = sum(confidence_scores) / len(confidence_scores)
        
        return analysis
    
    def prepare_results_for_export(self, original_df: pd.DataFrame, 
                                 research_results: Dict) -> pd.DataFrame:
        """
        Prepare enhanced dataframe for export with research results.
        
        Args:
            original_df (pd.DataFrame): Original CSV data
            research_results (Dict): Research session results
            
        Returns:
            pd.DataFrame: Enhanced dataframe ready for export
        """
        results = research_results.get('results', {})
        enhanced_df = self.results_manager.merge_with_original_data(original_df, results)
        
        # Add session metadata
        enhanced_df['Research_Session_ID'] = research_results.get('session_id', '')
        enhanced_df['Research_Date'] = research_results.get('start_time', '')[:10]  # Date only
        
        return enhanced_df


# Legacy compute functions (maintain compatibility with existing structure)
@st.cache_data(show_spinner="Computing results...")
def analyze_data(df: pd.DataFrame) -> dict:
    """Perform heavy computation or analysis. Cached for performance."""
    try:
        # Clean dataframe for safe processing
        cleaned_df = clean_dataframe_for_arrow(df)
        
        summary = {
            "rows": len(cleaned_df),
            "columns": list(cleaned_df.columns),
            "mean_values": {},
        }
        
        # Safely compute mean values for numeric columns
        try:
            numeric_df = cleaned_df.select_dtypes(include=['number'])
            if not numeric_df.empty:
                summary["mean_values"] = numeric_df.mean().to_dict()
        except Exception as e:
            st.warning(f"Could not compute mean values: {str(e)}")
            summary["mean_values"] = {}
        
        return summary
        
    except Exception as e:
        st.error(f"Error in data analysis: {str(e)}")
        return {
            "rows": 0,
            "columns": [],
            "mean_values": {},
            "error": str(e)
        }


@st.cache_resource
def expensive_model():
    """Simulate loading a heavy ML model or resource."""
    class FakeModel:
        def predict(self, x):
            return sum(x) / len(x)
    return FakeModel()


def perform_heavy_computation(data: pd.DataFrame, computation_type: str = "web_research") -> Dict:
    """
    Enhanced heavy computation function with web research capability.
    Maintains compatibility with existing compute.py structure.
    
    Args:
        data (pd.DataFrame): Input data
        computation_type (str): Type of computation to perform
        
    Returns:
        Dict: Computation results
    """
    if computation_type == "web_research":
        orchestrator = WebResearchOrchestrator()
        
        # Extract company names (adjust column name as needed)
        company_column = 'Consignee Name'  # Adjust based on your CSV structure
        if company_column not in data.columns:
            # Try common alternatives
            for alt_col in ['Company Name', 'Company', 'Consignee', 'Business Name']:
                if alt_col in data.columns:
                    company_column = alt_col
                    break
        
        if company_column in data.columns:
            company_names = data[company_column].dropna().unique().tolist()
            
            # Perform research with progress tracking
            def progress_callback(completed, total):
                if 'research_progress' in st.session_state:
                    st.session_state.research_progress = completed / total
            
            return orchestrator.perform_web_research(company_names, progress_callback)
        else:
            return {
                'status': 'failed',
                'error': f'Company name column not found. Available columns: {list(data.columns)}',
                'results': {}
            }
    
    # Fallback to original computation logic
    return {
        'status': 'completed',
        'computation_type': computation_type,
        'result': f"Computed {computation_type} for {len(data)} records"
    }


def analyze_computation_results(results: Dict) -> Dict:
    """
    Analyze computation results - enhanced for web research.
    
    Args:
        results (Dict): Computation results
        
    Returns:
        Dict: Analysis of results
    """
    if 'results' in results and isinstance(results['results'], dict):
        # This is a web research result
        orchestrator = WebResearchOrchestrator()
        return orchestrator.analyze_research_results(results)
    
    # Fallback analysis
    return {
        'status': results.get('status', 'unknown'),
        'total_items': len(results.get('result', [])) if isinstance(results.get('result'), list) else 1
    }


# Additional utility functions for Streamlit integration
def create_research_progress_tracker():
    """Create progress tracking components for Streamlit."""
    if 'research_progress' not in st.session_state:
        st.session_state.research_progress = 0.0
    
    if 'research_status' not in st.session_state:
        st.session_state.research_status = 'ready'


def update_research_status(status: str, message: str = ""):
    """Update research status in session state."""
    st.session_state.research_status = status
    if message:
        st.session_state.research_message = message


def get_research_summary_stats(research_results: Dict) -> Dict:
    """Get summary statistics for research results display."""
    if not research_results or 'results' not in research_results:
        return {'total': 0, 'successful': 0, 'failed': 0, 'success_rate': 0.0}
    
    results = research_results['results']
    total = len(results)
    successful = len([r for r in results.values() if r['status'] == 'found'])
    failed = total - successful
    success_rate = (successful / total * 100) if total > 0 else 0.0
    
    return {
        'total': total,
        'successful': successful,
        'failed': failed,
        'success_rate': success_rate
    }


# Dummy web search integration (replace with actual implementation)
def integrate_web_search_results(search_results: List[Dict], company_name: str) -> Dict:
    """
    Integrate dummy web search results into contact research format.
    Replace this with actual web search integration.
    
    Args:
        search_results (List[Dict]): Raw web search results
        company_name (str): Company name being researched
        
    Returns:
        Dict: Formatted contact research result
    """
    # This is a placeholder - implement actual integration logic here
    dummy_result = perform_dummy_web_search(f"{company_name} contact email phone")
    
    return {
        'company_name': company_name,
        'status': 'found',
        'contacts': [
            {
                'email': f'contact@{company_name.lower().replace(" ", "")}.com',
                'type': 'contact',
                'source': 'web_search_dummy',
                'verified': False
            }
        ],
        'website': f'https://www.{company_name.lower().replace(" ", "")}.com',
        'search_results': dummy_result,
        'confidence_score': 0.7
    }
