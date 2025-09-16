"""
Web Scraper Service for Business Contact Research
Enhanced with Tavily + Groq Integration for Timber/Wood Business Research
Teakwood Business Web Scraping - Stage 4 Implementation
"""

import pandas as pd
import time
import streamlit as st

# Configure pandas to avoid FutureWarnings about dtype incompatibility
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')
from typing import Dict, List, Tuple, Optional, Callable
import random
import json
from datetime import datetime
import os
import asyncio
import requests
import logging
from dotenv import load_dotenv
from tavily import TavilyClient
import re
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# Load environment variables with multiple fallback strategies
try:
    # Strategy 1: Try importing simple direct loader
    import sys
    from pathlib import Path
    
    # Add current directory to path
    current_dir = Path(__file__).parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from simple_env import load_env_direct, verify_env
    print("✅ Using direct environment loading")
    verify_env()
    
except ImportError:
    print("⚠️ Direct loader not found, trying dotenv...")
    try:
        # Strategy 2: Try dotenv with explicit path
        from pathlib import Path
        env_path = Path(__file__).parent.parent / '.env'
        if env_path.exists():
            load_dotenv(env_path)
            print(f"✅ Loaded .env from: {env_path}")
        else:
            load_dotenv()
            print("⚠️ Using default dotenv loading")
    except Exception as e:
        print(f"⚠️ Dotenv error: {e}")
        # Strategy 3: Manual environment setting as last resort
        os.environ['GROQ_API_KEY'] = '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'
        os.environ['TAVILY_API_KEY'] = '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'
        print("✅ Using manual environment setting")
        
except Exception as e:
    print(f"⚠️ Environment loading error: {e}")
    # Final fallback
    os.environ['GROQ_API_KEY'] = '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'
    os.environ['TAVILY_API_KEY'] = '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'
    print("✅ Using final fallback environment setting")

# Import search configuration
try:
    from .search_config import SEARCH_LAYERS_CONFIG, API_CONFIG, PREFERRED_DOMAINS, get_enabled_layers, get_search_summary
except ImportError:
    # Fallback configuration if search_config not available
    SEARCH_LAYERS_CONFIG = {
        'enable_general_search': True,
        'enable_government_search': True,
        'enable_industry_search': True,
    }
    API_CONFIG = {
        'groq_model': 'llama-3.3-70b-versatile',
        'search_delay': 2.0,
        'max_retries': 3,
        'timeout': 60
    }
    PREFERRED_DOMAINS = {
        "Government": ["gov.in", "nic.in"],
        "Industry": ["fidr.org", "cii.in"]
    }
    
    def get_enabled_layers():
        return ['General', 'Government', 'Industry']
    
    def get_search_summary():
        return "Comprehensive search (all layers)"


class DirectWebsiteScraper:
    """
    Direct website scraping for contact information extraction
    Used when website is found but email is missing from search results
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        # Email regex patterns
        self.email_patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            r'\b[a-zA-Z0-9._%+-]+\s*@\s*[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
            r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        ]
        
        # Phone regex patterns (Indian and international)
        self.phone_patterns = [
            r'\+91[-\s]?\d{10}',  # Indian format with +91
            r'\b\d{10}\b',  # 10 digit Indian mobile
            r'\b\d{3}[-.]?\s?\d{3}[-.]?\s?\d{4}\b',  # US format
            r'\(\d{3}\)\s*\d{3}[-.]?\s?\d{4}',  # (123) 123-1234
            r'\b\d{2,4}[-.]?\s?\d{2,4}[-.]?\s?\d{2,4}[-.]?\s?\d{2,4}\b',  # General international
        ]
        
        # Common contact page paths
        self.contact_paths = [
            '/contact',
            '/contact-us',
            '/contactus',
            '/about',
            '/about-us',
            '/aboutus',
            '/reach-us',
            '/get-in-touch',
            '/contact-info',
            '/contact-information',
            '/support',
            '/help',
            '/connect',
        ]
    
    def is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and accessible"""
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc])
        except:
            return False
    
    def clean_url(self, url: str) -> str:
        """Clean and normalize URL"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.strip().rstrip('/')
    
    def fetch_page_content(self, url: str, timeout: int = 10) -> Optional[str]:
        """Fetch webpage content with error handling"""
        try:
            print(f"      🌐 Fetching: {url}")
            response = self.session.get(url, timeout=timeout, allow_redirects=True)
            
            if response.status_code == 200:
                print(f"      ✅ Fetched successfully ({len(response.content)} bytes)")
                return response.text
            else:
                print(f"      ❌ HTTP {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            print(f"      ⏰ Timeout after {timeout}s")
            return None
        except requests.exceptions.ConnectionError:
            print(f"      🔌 Connection error")
            return None
        except Exception as e:
            print(f"      ⚠️ Error: {str(e)[:50]}")
            return None
    
    def extract_emails_from_text(self, text: str) -> List[str]:
        """Extract email addresses from text using regex patterns"""
        emails = set()
        
        for pattern in self.email_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]  # For mailto: pattern
                
                # Basic email validation
                if '@' in match and '.' in match.split('@')[1]:
                    # Clean up common issues
                    email = match.lower().strip()
                    # Remove common false positives
                    if not any(exclude in email for exclude in [
                        'example.com', 'test.com', 'domain.com', 'yoursite.com',
                        'website.com', 'email.com', 'sample.com', 'demo.com',
                        '.png', '.jpg', '.jpeg', '.gif', '.pdf', '.doc'
                    ]):
                        emails.add(email)
        
        return list(emails)
    
    def extract_phones_from_text(self, text: str) -> List[str]:
        """Extract phone numbers from text using regex patterns"""
        phones = set()
        
        for pattern in self.phone_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Clean phone number
                phone = re.sub(r'[^\d+]', '', match)
                if len(phone) >= 10:  # Minimum valid phone length
                    phones.add(match.strip())
        
        return list(phones)
    
    def parse_contact_info_from_html(self, html_content: str, base_url: str) -> Dict:
        """Parse contact information from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()
            
            # Get text content
            text_content = soup.get_text()
            
            # Extract emails and phones
            emails = self.extract_emails_from_text(text_content)
            phones = self.extract_phones_from_text(text_content)
            
            # Look for contact-specific elements
            contact_info = {
                'emails': emails,
                'phones': phones,
                'address': self.extract_address_from_html(soup),
                'social_links': self.extract_social_links(soup, base_url),
                'contact_forms': self.find_contact_forms(soup),
            }
            
            return contact_info
            
        except Exception as e:
            print(f"      ⚠️ HTML parsing error: {e}")
            return {'emails': [], 'phones': [], 'address': '', 'social_links': [], 'contact_forms': []}
    
    def extract_address_from_html(self, soup: BeautifulSoup) -> str:
        """Extract address information from HTML"""
        address_indicators = [
            'address', 'location', 'office', 'headquarters', 'head office',
            'corporate office', 'registered office', 'postal address'
        ]
        
        addresses = []
        
        # Look for address in structured data
        for element in soup.find_all(['div', 'p', 'span'], class_=True):
            class_name = ' '.join(element.get('class', [])).lower()
            if any(indicator in class_name for indicator in address_indicators):
                text = element.get_text(strip=True)
                if len(text) > 20 and any(word in text.lower() for word in ['road', 'street', 'city', 'state', 'pin', 'zip']):
                    addresses.append(text)
        
        # Look for address in text content
        text_content = soup.get_text()
        address_patterns = [
            r'(?i)address[:\s-]*([^\n]{20,100})',
            r'(?i)office[:\s-]*([^\n]{20,100})',
            r'(?i)location[:\s-]*([^\n]{20,100})',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text_content)
            addresses.extend([match.strip() for match in matches if len(match.strip()) > 20])
        
        return addresses[0] if addresses else ''
    
    def extract_social_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract social media and contact links"""
        social_platforms = ['facebook', 'twitter', 'linkedin', 'instagram', 'youtube', 'whatsapp']
        social_links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            
            if any(platform in full_url.lower() for platform in social_platforms):
                social_links.append(full_url)
        
        return list(set(social_links))
    
    def find_contact_forms(self, soup: BeautifulSoup) -> List[str]:
        """Find contact forms on the page"""
        forms = []
        
        for form in soup.find_all('form'):
            # Look for contact-related forms
            form_text = form.get_text().lower()
            if any(keyword in form_text for keyword in ['contact', 'inquiry', 'message', 'get in touch', 'reach us']):
                action = form.get('action', '')
                if action:
                    forms.append(action)
        
        return forms
    
    def scrape_website_for_contacts(self, website_url: str, business_name: str) -> Dict:
        """
        Main method to scrape a website for contact information
        """
        print(f"   🕷️ Direct website scraping for: {business_name}")
        print(f"   🌐 Website: {website_url}")
        
        website_url = self.clean_url(website_url)
        
        if not self.is_valid_url(website_url):
            print(f"   ❌ Invalid URL: {website_url}")
            return self.create_empty_result()
        
        all_contacts = {
            'emails': set(),
            'phones': set(),
            'addresses': [],
            'social_links': [],
            'contact_forms': [],
            'pages_scraped': []
        }
        
        # List of URLs to try
        urls_to_scrape = [website_url]  # Start with main page
        
        # Add contact page variations
        base_domain = f"{urlparse(website_url).scheme}://{urlparse(website_url).netloc}"
        for path in self.contact_paths:
            contact_url = base_domain + path
            urls_to_scrape.append(contact_url)
        
        # Scrape each URL
        success_count = 0
        for url in urls_to_scrape[:5]:  # Limit to 5 pages to avoid being too aggressive
            content = self.fetch_page_content(url)
            
            if content:
                success_count += 1
                contact_info = self.parse_contact_info_from_html(content, website_url)
                
                # Merge results
                all_contacts['emails'].update(contact_info['emails'])
                all_contacts['phones'].update(contact_info['phones'])
                if contact_info['address']:
                    all_contacts['addresses'].append(contact_info['address'])
                all_contacts['social_links'].extend(contact_info['social_links'])
                all_contacts['contact_forms'].extend(contact_info['contact_forms'])
                all_contacts['pages_scraped'].append(url)
                
                print(f"      📊 Found: {len(contact_info['emails'])} emails, {len(contact_info['phones'])} phones")
            
            # Small delay between requests
            time.sleep(1)
        
        # Convert sets to lists and clean up
        result = {
            'emails': list(all_contacts['emails']),
            'phones': list(all_contacts['phones']),
            'address': all_contacts['addresses'][0] if all_contacts['addresses'] else '',
            'social_links': list(set(all_contacts['social_links'])),
            'contact_forms': list(set(all_contacts['contact_forms'])),
            'pages_scraped': all_contacts['pages_scraped'],
            'success_count': success_count,
            'scraping_successful': success_count > 0
        }
        
        print(f"   📋 Website scraping results:")
        print(f"      📧 Emails found: {len(result['emails'])}")
        print(f"      📞 Phones found: {len(result['phones'])}")
        print(f"      📍 Address: {'Yes' if result['address'] else 'No'}")
        print(f"      🔗 Social links: {len(result['social_links'])}")
        print(f"      📄 Pages scraped: {success_count}/{len(urls_to_scrape[:5])}")
        
        return result
    
    def create_empty_result(self) -> Dict:
        """Create empty result structure"""
        return {
            'emails': [],
            'phones': [],
            'address': '',
            'social_links': [],
            'contact_forms': [],
            'pages_scraped': [],
            'success_count': 0,
            'scraping_successful': False
        }


class TimberwoodBusinessResearcher:
    """
    Advanced web scraper for timber/wood business contact research
    using Tavily search engine and Groq AI for data extraction
    """
    
    def __init__(self):
        # Load API keys
        self.tavily_key = self.get_env_var('TAVILY_API_KEY')
        self.groq_key = self.get_env_var('GROQ_API_KEY')
        
        # Validate API keys
        if not self.tavily_key or not self.groq_key:
            st.error("❌ Missing API Keys! Please configure TAVILY_API_KEY and GROQ_API_KEY in your .env file")
            raise ValueError("API Keys not configured")
        
        # Initialize clients
        self.tavily_client = TavilyClient(api_key=self.tavily_key)
        self.website_scraper = DirectWebsiteScraper()  # Initialize direct website scraper
        
        # Configuration from search_config
        self.search_delay = API_CONFIG.get('search_delay', 2.0)
        self.max_retries = API_CONFIG.get('max_retries', 3)
        self.timeout = API_CONFIG.get('timeout', 60)
        self.groq_model = API_CONFIG.get('groq_model', 'llama-3.3-70b-versatile')
        
        # Results storage
        self.results = []
    
    def get_env_var(self, key, default=None):
        """Get environment variable with enhanced debugging and Streamlit compatibility"""
        print(f"🔍 Looking for environment variable: {key}")
        
        # Method 1: Try regular environment variables
        value = os.getenv(key)
        if value:
            print(f"✅ Found {key} via os.getenv()")
            return value
        else:
            print(f"❌ {key} not found via os.getenv()")
        
        # Method 2: Try os.environ directly
        value = os.environ.get(key)
        if value:
            print(f"✅ Found {key} via os.environ.get()")
            return value
        else:
            print(f"❌ {key} not found via os.environ.get()")
        
        # Method 3: Try Streamlit secrets
        try:
            if hasattr(st, 'secrets') and key in st.secrets:
                print(f"✅ Found {key} via Streamlit secrets")
                return st.secrets[key]
            else:
                print(f"❌ {key} not found in Streamlit secrets")
        except Exception as e:
            print(f"❌ Error accessing Streamlit secrets: {e}")
        
        # Method 4: Check all environment variables for debugging
        print(f"🔍 Available env vars starting with {key[:4]}: {[k for k in os.environ.keys() if k.startswith(key[:4])]}")
        
        print(f"❌ {key} not found anywhere, using default: {default}")
        return default
    
    def test_apis(self):
        """Test both Tavily and Groq APIs"""
        print("🧪 Testing APIs...")
        
        # Test Groq
        try:
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.groq_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.groq_model,
                    "messages": [{"role": "user", "content": "Say 'Groq working'"}],
                    "max_tokens": 10,
                    "temperature": 0.1
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('choices') and result['choices'][0].get('message', {}).get('content'):
                    print("✅ Groq API: Working")
                else:
                    return False, "Groq API: Empty response"
            else:
                return False, f"Groq API: HTTP {response.status_code}"
                
        except Exception as e:
            return False, f"Groq API Error: {e}"
        
        # Test Tavily
        try:
            response = self.tavily_client.search("test timber business", max_results=1)
            if response.get('results'):
                print("✅ Tavily API: Working")
            else:
                return False, "Tavily API: No results"
                
        except Exception as e:
            return False, f"Tavily API Error: {e}"
        
        return True, "All APIs working"
    
    async def research_business_comprehensive(self, business_name: str, expected_city: str = None) -> Dict:
        """
        Comprehensive research for a timber/wood business using configurable multi-layer search
        """
        print(f"🔍 Researching: {business_name}")
        print(f"⚙️ Search config: {get_search_summary()}")
        
        try:
            # Multi-layer configurable search strategy
            search_results = []
            
            # Layer 1: General timber business search (always enabled)
            if SEARCH_LAYERS_CONFIG.get('enable_general_search', True):
                print("   📊 Layer 1: General timber business search...")
                general_results = self.search_timber_business_info(business_name)
                search_results.extend(general_results)
            else:
                print("   ❌ Layer 1: General search disabled")
            
            # Layer 2: Government sources (configurable)
            if SEARCH_LAYERS_CONFIG.get('enable_government_search', False):
                print("   🏛️ Layer 2: Government sources search...")
                government_results = self.search_government_sources(business_name)
                search_results.extend(government_results)
            else:
                print("   ❌ Layer 2: Government search disabled")
            
            # Layer 3: Industry-specific sources (configurable)
            if SEARCH_LAYERS_CONFIG.get('enable_industry_search', False):
                print("   🌲 Layer 3: Timber industry sources...")
                industry_results = self.search_industry_sources(business_name)
                search_results.extend(industry_results)
            else:
                print("   ❌ Layer 3: Industry search disabled")
            
            if not search_results:
                print(f"❌ No search results found for {business_name}")
                return self.create_manual_fallback(business_name)
            
            # Extract contact info using Groq AI
            contact_info = await self.extract_contacts_with_groq(
                business_name, search_results, expected_city
            )
            
            return contact_info
            
        except Exception as e:
            print(f"❌ Error researching {business_name}: {e}")
            return self.create_manual_fallback(business_name)
    
    def merge_direct_scraping_results(self, original_extracted_info: str, scraping_results: Dict) -> str:
        """
        Merge direct website scraping results with original extracted info
        """
        print(f"   🔄 Merging direct scraping results...")
        
        # Parse the original extracted info to update specific fields
        lines = original_extracted_info.split('\n')
        updated_lines = []
        
        emails_found = scraping_results.get('emails', [])
        phones_found = scraping_results.get('phones', [])
        address_found = scraping_results.get('address', '')
        
        for line in lines:
            line_updated = False
            
            # Update EMAIL field if we found emails from scraping
            if line.strip().startswith('EMAIL:') and emails_found:
                current_email = line.replace('EMAIL:', '').strip()
                if current_email in ['Not found', 'Research required', '']:
                    updated_lines.append(f"EMAIL: {emails_found[0]} (via direct scraping)")
                    line_updated = True
                    print(f"      📧 Updated email: {emails_found[0]}")
            
            # Update PHONE field if we found phones from scraping
            elif line.strip().startswith('PHONE:') and phones_found:
                current_phone = line.replace('PHONE:', '').strip()
                if current_phone in ['Not found', 'Research required', '']:
                    updated_lines.append(f"PHONE: {phones_found[0]} (via direct scraping)")
                    line_updated = True
                    print(f"      📞 Updated phone: {phones_found[0]}")
            
            # Update ADDRESS field if we found address from scraping
            elif line.strip().startswith('ADDRESS:') and address_found:
                current_address = line.replace('ADDRESS:', '').strip()
                if current_address in ['Not found', 'Research required', '']:
                    # Truncate long addresses for display
                    display_address = address_found[:80] + "..." if len(address_found) > 80 else address_found
                    updated_lines.append(f"ADDRESS: {display_address} (via direct scraping)")
                    line_updated = True
                    print(f"      📍 Updated address: {display_address}")
            
            # Keep original line if not updated
            if not line_updated:
                updated_lines.append(line)
        
        # Add summary of direct scraping at the end
        if emails_found or phones_found or address_found:
            updated_lines.append("")
            updated_lines.append("DIRECT_SCRAPING_SUMMARY:")
            updated_lines.append(f"- Emails found: {len(emails_found)} {emails_found}")
            updated_lines.append(f"- Phones found: {len(phones_found)} {phones_found}")
            updated_lines.append(f"- Address found: {'Yes' if address_found else 'No'}")
            updated_lines.append(f"- Pages scraped: {scraping_results.get('success_count', 0)}")
            updated_lines.append(f"- Social links: {len(scraping_results.get('social_links', []))}")
        
        return '\n'.join(updated_lines)
    
    def search_timber_business_info(self, business_name: str) -> List[Dict]:
        """Search for general timber/wood business information"""
        search_queries = [
            f"{business_name} timber wood teak contact information phone email",
            f"{business_name} lumber plywood business address website",
            f"{business_name} wood trading company contact details"
        ]
        
        return self.execute_search_queries(search_queries, "General")
    
    def search_government_sources(self, business_name: str) -> List[Dict]:
        """Search government databases for timber business registration"""
        government_queries = [
            f'"{business_name}" site:gov.in business registration timber',
            f'"{business_name}" GST registration wood lumber',
            f'"{business_name}" forest department license timber'
        ]
        
        return self.execute_search_queries(government_queries, "Government")
    
    def search_industry_sources(self, business_name: str) -> List[Dict]:
        """Search timber industry specific sources"""
        industry_queries = [
            f'"{business_name}" timber traders association member',
            f'"{business_name}" wood importers exporters directory',
            f'"{business_name}" timber merchants federation'
        ]
        
        return self.execute_search_queries(industry_queries, "Industry")
    
    def execute_search_queries(self, queries: List[str], search_type: str) -> List[Dict]:
        """Execute search queries using Tavily with domain preferences"""
        all_results = []
        
        # Get preferred domains for this search type
        include_domains = PREFERRED_DOMAINS.get(search_type)
        
        for query in queries:
            try:
                print(f"      🔍 {search_type}: {query[:60]}...")
                
                # Configure search parameters
                search_params = {
                    "query": query,
                    "max_results": 2,
                    "search_depth": "advanced"
                }
                
                # Add domain preferences if available
                if include_domains:
                    search_params["include_domains"] = include_domains
                
                response = self.tavily_client.search(**search_params)
                
                if response.get('results'):
                    for result in response['results']:
                        result['search_type'] = search_type
                    all_results.extend(response['results'])
                    print(f"         ✅ Found {len(response['results'])} results")
                else:
                    print(f"         ❌ No results")
                    
            except Exception as e:
                print(f"         ⚠️ Error: {str(e)[:50]}")
                
        print(f"   📊 {search_type} total: {len(all_results)} results")
        return all_results
    
    async def extract_contacts_with_groq(self, business_name: str, search_results: List[Dict], expected_city: str = None) -> Dict:
        """Extract contact information using Groq AI with timber business focus"""
        print(f"   🦙 Analyzing {len(search_results)} results with Groq...")
        
        # Categorize results by source type  
        categorized_results = self.categorize_search_results(search_results)
        
        # Format search results for Groq analysis
        results_text = self.format_search_results_for_groq(categorized_results)
        
        # Build location context
        location_context = ""
        if expected_city:
            location_context = f"""
EXPECTED LOCATION: {expected_city}
LOCATION VERIFICATION: Verify if the business address matches the expected city.
"""
        
        # Count sources by type
        govt_sources = len(categorized_results.get('Government', []))
        industry_sources = len(categorized_results.get('Industry', []))
        
        prompt = f"""You are analyzing comprehensive search results for TIMBER, WOOD, TEAK, LUMBER, and PLYWOOD businesses.
Results include {govt_sources} government sources, {industry_sources} industry sources, and general web sources.

BUSINESS TO RESEARCH: "{business_name}"

{location_context}

COMPREHENSIVE SEARCH RESULTS:
{results_text}

INSTRUCTIONS:
1. Focus ONLY on businesses related to timber, wood, teak, lumber, plywood industry
2. Extract complete business information with contact details
3. Verify business relevance to wood/timber industry
4. Prioritize information from government sources (.gov.in domains)
5. Cross-verify information across multiple source types

EXTRACT AND FORMAT:
BUSINESS_NAME: {business_name}
INDUSTRY_RELEVANT: [YES/NO - Is this related to wood/timber industry?]
LOCATION_RELEVANT: [YES/NO/UNKNOWN - Does location match expected city?]
PHONE: [extract phone number or "Not found"]
EMAIL: [extract email address or "Not found"]
WEBSITE: [extract website URL or "Not found"]
ADDRESS: [extract business address or "Not found"]
CITY: [extract city or "Not found"]
REGISTRATION_NUMBER: [extract company registration/GST number if found in government sources, or "Not found"]
LICENSE_DETAILS: [extract any timber/forest licenses mentioned, or "Not found"]
DESCRIPTION: [brief business description focusing on timber/wood activities based on all sources]
GOVERNMENT_VERIFIED: [YES if found in government sources, NO if only general/industry sources]
CONFIDENCE: [rate 1-10 based on quality, number of sources, and government verification]
RELEVANCE_NOTES: [explain why this business is relevant to timber industry and source quality]

STRICT RULES:
1. Only extract information if INDUSTRY_RELEVANT = YES
2. If not timber/wood related, set all fields to "Not relevant - not timber business"  
3. Prioritize information from government sources over other sources
4. Mark GOVERNMENT_VERIFIED = YES only if found in .gov.in domains
5. Higher confidence for government-verified businesses

Format your response exactly as shown above with the field names.
"""
        
        try:
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.groq_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.groq_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 1200,
                    "temperature": 0.1
                },
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('choices') and result['choices'][0].get('message', {}).get('content'):
                    extracted_info = result['choices'][0]['message']['content']
                    print(f"   ✅ Groq extraction completed")
                    
                    # 🚀 NEW: Check if we need direct website scraping
                    extracted_website = self.extract_field_value(extracted_info, 'WEBSITE:')
                    extracted_email = self.extract_field_value(extracted_info, 'EMAIL:')
                    industry_relevant = self.extract_field_value(extracted_info, 'INDUSTRY_RELEVANT:')
                    
                    # If website found but no email, and business is relevant, do direct scraping
                    direct_scraping_results = None
                    if (extracted_website and 
                        extracted_website not in ['Not found', 'Research required', ''] and
                        (not extracted_email or extracted_email in ['Not found', 'Research required', '']) and
                        industry_relevant == 'YES'):
                        
                        print(f"   🎯 Website found but email missing - initiating direct scraping...")
                        direct_scraping_results = self.website_scraper.scrape_website_for_contacts(
                            extracted_website, business_name
                        )
                        
                        # If direct scraping found additional contacts, update the extracted info
                        if direct_scraping_results and direct_scraping_results['scraping_successful']:
                            extracted_info = self.merge_direct_scraping_results(
                                extracted_info, direct_scraping_results
                            )
                            print(f"   🎉 Direct scraping enhanced the results!")
                        else:
                            print(f"   📝 Direct scraping completed but no additional contacts found")
                    
                    # Create result data structure
                    result_data = {
                        'business_name': business_name,
                        'extracted_info': extracted_info,
                        'search_results': search_results,
                        'direct_scraping_results': direct_scraping_results,  # New field
                        'government_sources_found': govt_sources,
                        'industry_sources_found': industry_sources,
                        'total_sources': len(search_results),
                        'research_date': datetime.now().isoformat(),
                        'method': 'Enhanced Tavily + Groq + Direct Website Scraping',  # Updated method
                        'status': 'success'
                    }
                    
                    self.results.append(result_data)
                    
                    # Display results
                    print(f"   📋 Results for {business_name}:")
                    print("-" * 60)
                    print(extracted_info)
                    print("-" * 60)
                    print(f"   📊 Sources: {govt_sources} govt, {industry_sources} industry, {len(search_results)} total")
                    if direct_scraping_results:
                        print(f"   🕷️ Direct scraping: {direct_scraping_results['success_count']} pages scraped")
                    
                    return result_data
                else:
                    return self.create_manual_fallback(business_name)
            else:
                print(f"   ❌ Groq API error: HTTP {response.status_code}")
                return self.create_manual_fallback(business_name)
                
        except Exception as e:
            print(f"   ❌ Groq extraction error: {e}")
            return self.create_manual_fallback(business_name)
    
    def categorize_search_results(self, search_results: List[Dict]) -> Dict:
        """Categorize results by source type"""
        categorized = {
            'Government': [],
            'Industry': [], 
            'General': []
        }
        
        for result in search_results:
            search_type = result.get('search_type', 'General')
            categorized[search_type].append(result)
        
        return categorized
    
    def format_search_results_for_groq(self, categorized_results: Dict) -> str:
        """Format categorized search results for enhanced analysis"""
        formatted_sections = []
        
        for category, results in categorized_results.items():
            if results:
                formatted_sections.append(f"\n=== {category.upper()} SOURCES ===")
                
                for i, result in enumerate(results[:4], 1):  # Top 4 per category
                    formatted_result = f"""
{category.upper()} RESULT {i}:
Title: {result.get('title', 'No title')}
URL: {result.get('url', 'No URL')}
Content: {result.get('content', 'No content')[:400]}...
"""
                    formatted_sections.append(formatted_result)
        
        return '\n'.join(formatted_sections)
    
    def create_manual_fallback(self, business_name: str) -> Dict:
        """Create manual fallback result with enhanced guidance"""
        fallback_info = f"""
BUSINESS_NAME: {business_name}
INDUSTRY_RELEVANT: UNKNOWN
LOCATION_RELEVANT: UNKNOWN
PHONE: Research required
EMAIL: Research required
WEBSITE: Research required
ADDRESS: Research required
CITY: Research required
REGISTRATION_NUMBER: Research required
LICENSE_DETAILS: Research required
DESCRIPTION: Manual verification needed for timber/wood business relevance
GOVERNMENT_VERIFIED: NO - manual verification needed
CONFIDENCE: 1
RELEVANCE_NOTES: Automated research failed - comprehensive manual verification needed

COMPREHENSIVE MANUAL RESEARCH NEEDED:

Government Sources:
1. MCA Portal: https://www.mca.gov.in/ (Company registration details)
2. GST Portal: https://gst.gov.in/ (GST registration and verification)  
3. State Forest Department websites (Timber trading licenses)
4. Forest Survey of India: https://fsi.nic.in/ (Forest clearances)

Industry Sources:
5. FIDR: https://fidr.org/ (Forest Industries Directory)
6. Timber Traders Association directories (State-wise)
7. Chamber of Commerce member lists (CII, FICCI, ASSOCHAM)

General Sources:
8. Google: "{business_name}" + "wood" OR "timber" OR "teak" contact
9. Business directories and Yellow Pages
10. LinkedIn company profiles for wood/timber businesses
"""
        
        result = {
            'business_name': business_name,
            'extracted_info': fallback_info,
            'search_results': [],
            'government_sources_found': 0,
            'industry_sources_found': 0,
            'total_sources': 0,
            'research_date': datetime.now().isoformat(),
            'method': 'Enhanced Manual Fallback Required',
            'status': 'manual_required'
        }
        
        self.results.append(result)
        return result
    
    def get_results_dataframe(self) -> pd.DataFrame:
        """Convert results to DataFrame format"""
        if not self.results:
            return pd.DataFrame()
        
        csv_data = []
        for result in self.results:
            csv_row = self.parse_extracted_info_to_csv(result)
            csv_data.append(csv_row)
        
        return pd.DataFrame(csv_data)
    
    def parse_extracted_info_to_csv(self, result: Dict) -> Dict:
        """Parse extracted info into CSV format"""
        info = result['extracted_info']
        business_name = result['business_name']
        
        csv_row = {
            'business_name': business_name,
            'industry_relevant': self.extract_field_value(info, 'INDUSTRY_RELEVANT:'),
            'location_relevant': self.extract_field_value(info, 'LOCATION_RELEVANT:'),
            'phone': self.extract_field_value(info, 'PHONE:'),
            'email': self.extract_field_value(info, 'EMAIL:'),
            'website': self.extract_field_value(info, 'WEBSITE:'),
            'address': self.extract_field_value(info, 'ADDRESS:'),
            'city': self.extract_field_value(info, 'CITY:'),
            'registration_number': self.extract_field_value(info, 'REGISTRATION_NUMBER:'),
            'license_details': self.extract_field_value(info, 'LICENSE_DETAILS:'),
            'description': self.extract_field_value(info, 'DESCRIPTION:'),
            'government_verified': self.extract_field_value(info, 'GOVERNMENT_VERIFIED:'),
            'confidence': self.extract_field_value(info, 'CONFIDENCE:'),
            'relevance_notes': self.extract_field_value(info, 'RELEVANCE_NOTES:'),
            'government_sources_found': result.get('government_sources_found', 0),
            'industry_sources_found': result.get('industry_sources_found', 0),
            'total_sources': result.get('total_sources', 0),
            'research_date': result['research_date'],
            'method': result['method'],
            'status': result['status']
        }
        
        return csv_row
    
    def extract_field_value(self, text: str, field_name: str) -> str:
        """Extract field value from formatted text"""
        try:
            lines = text.split('\n')
            for line in lines:
                if line.strip().startswith(field_name):
                    value = line.replace(field_name, '').strip()
                    return value if value and value != "Not found" else ""
            return ""
        except:
            return ""


class WebScraper:
    """
    Legacy WebScraper class - now uses TimberwoodBusinessResearcher
    Maintains compatibility with existing code structure
    """
    
    def __init__(self):
        self.search_delay = API_CONFIG.get('search_delay', 1.0)
        self.max_retries = API_CONFIG.get('max_retries', 3)
        self.timeout = API_CONFIG.get('timeout', 30)
        
        # Initialize the enhanced researcher
        try:
            self.researcher = TimberwoodBusinessResearcher()
        except Exception as e:
            st.error(f"Failed to initialize enhanced researcher: {e}")
            self.researcher = None
        
    def research_company_contacts(self, company_name: str, expected_city: str = None) -> Dict:
        """
        Research contact details for a single company using enhanced scraper
        """
        if self.researcher is None:
            return self.create_fallback_result(company_name, "API not configured")
        
        try:
            # Run async research
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    self.researcher.research_business_comprehensive(company_name, expected_city)
                )
                return self.convert_to_legacy_format(result)
            finally:
                loop.close()
                
        except Exception as e:
            return self.create_fallback_result(company_name, str(e))
    
    def convert_to_legacy_format(self, enhanced_result: Dict) -> Dict:
        """Convert enhanced result to legacy format"""
        extracted_info = enhanced_result.get('extracted_info', '')
        
        # Parse extracted info
        industry_relevant = self.researcher.extract_field_value(extracted_info, 'INDUSTRY_RELEVANT:')
        email = self.researcher.extract_field_value(extracted_info, 'EMAIL:')
        phone = self.researcher.extract_field_value(extracted_info, 'PHONE:')
        website = self.researcher.extract_field_value(extracted_info, 'WEBSITE:')
        description = self.researcher.extract_field_value(extracted_info, 'DESCRIPTION:')
        
        # Create legacy format
        contacts = []
        if email and email not in ['Not found', 'Research required', 'Not relevant']:
            contacts.append({
                'email': email,
                'type': 'primary',
                'phone': phone if phone not in ['Not found', 'Research required'] else None,
                'verified': True
            })
        
        status = 'found' if (industry_relevant == 'YES' and contacts) else 'not_found'
        
        return {
            'company_name': enhanced_result['business_name'],
            'status': status,
            'contacts': contacts,
            'website': website if website not in ['Not found', 'Research required'] else None,
            'description': description,  # Added description field
            'search_timestamp': enhanced_result['research_date'],
            'confidence_score': 0.8 if status == 'found' else 0.2,
            'method': enhanced_result['method']
        }
    
    def create_fallback_result(self, company_name: str, error_msg: str) -> Dict:
        """Create fallback result for errors"""
        return {
            'company_name': company_name,
            'status': 'error',
            'contacts': [],
            'website': None,
            'description': f"Research failed: {error_msg}",  # Added description
            'error_message': error_msg,
            'search_timestamp': datetime.now().isoformat(),
            'confidence_score': 0.0
        }
    
    def batch_research_with_progress(self, company_list: List[str], 
                                   progress_callback: Optional[Callable] = None) -> Dict:
        """Perform batch research with progress tracking"""
        results = {}
        total_companies = len(company_list)
        
        for i, company_name in enumerate(company_list):
            try:
                result = self.research_company_contacts(company_name)
                results[company_name] = result
                
                if progress_callback:
                    progress_callback(i + 1, total_companies)
                
                # Delay between requests
                time.sleep(self.search_delay)
                
            except Exception as e:
                results[company_name] = self.create_fallback_result(company_name, str(e))
                
        return results
    
    def test_api_connection(self) -> Tuple[bool, str]:
        """Test API connection"""
        if self.researcher is None:
            return False, "Enhanced researcher not initialized"
        
        return self.researcher.test_apis()


class ResearchResultsManager:
    """
    Enhanced results manager with timber business focus
    """
    
    @staticmethod
    def format_results_for_display(results: Dict) -> pd.DataFrame:
        """Format research results for display"""
        formatted_data = []
        
        for company_name, result in results.items():
            if result['status'] == 'found':
                contacts = result.get('contacts', [])
                primary_email = contacts[0]['email'] if contacts else 'Not found'
                phone = contacts[0]['phone'] if contacts and contacts[0].get('phone') else 'Not found'
                website = result.get('website', 'Not found')
                description = result.get('description', 'No description')  # Added description
                confidence = f"{result.get('confidence_score', 0):.2%}"
            else:
                primary_email = 'Research failed'
                phone = 'Research failed'
                website = 'Research failed'
                description = result.get('description', 'Research failed')  # Added description
                confidence = '0%'
            
            formatted_data.append({
                'Company Name': company_name,
                'Status': result['status'].title(),
                'Primary Email': primary_email,
                'Phone': phone,
                'Website': website,
                'Description': description,  # Added description column
                'Confidence': confidence,
                'Contacts Found': len(result.get('contacts', [])),
                'Timestamp': result.get('search_timestamp', '')
            })
        
        return pd.DataFrame(formatted_data)
    
    @staticmethod
    def merge_with_original_data(original_df: pd.DataFrame, results: Dict) -> pd.DataFrame:
        """
        INCREMENTAL MERGE: Preserve existing research data and only update new results
        This prevents overwriting previously researched businesses
        """
        enhanced_df = original_df.copy()
        
        # Add research result columns ONLY if they don't exist (preserve existing data)
        # Use appropriate dtypes to prevent FutureWarning
        research_columns = {
            'Research_Status': ('pending', 'object'),
            'Primary_Email': ('', 'object'),
            'Phone_Number': ('', 'object'),
            'Website': ('', 'object'),
            'Business_Description': ('', 'object'),
            'Research_Confidence': (0.0, 'float64'),
            'Research_Timestamp': ('', 'object')
        }
        
        # Add missing columns with default values and correct dtypes, but preserve existing data
        for col_name, (default_value, dtype) in research_columns.items():
            if col_name not in enhanced_df.columns:
                enhanced_df[col_name] = pd.Series([default_value] * len(enhanced_df), dtype=dtype)
                print(f"   📝 Added new column: {col_name} ({dtype})")
            else:
                # Ensure existing columns have correct dtype
                if enhanced_df[col_name].dtype != dtype and col_name != 'Research_Confidence':
                    enhanced_df[col_name] = enhanced_df[col_name].astype(dtype)
                    print(f"   🔄 Fixed dtype for existing column: {col_name} -> {dtype}")
                else:
                    print(f"   ✅ Preserved existing column: {col_name}")
        
        # Find company column
        company_column = None
        for col in ['Consignee Name', 'Company Name', 'Company', 'Business Name']:
            if col in enhanced_df.columns:
                company_column = col
                break
        
        if not company_column:
            print("   ❌ No company column found for merging")
            return enhanced_df
        
        print(f"   📊 Using company column: {company_column}")
        
        # INCREMENTAL UPDATE: Only update rows for companies that were just researched
        updated_count = 0
        preserved_count = 0
        
        for index, row in enhanced_df.iterrows():
            company_name = row.get(company_column, '')
            
            # Check if this company was researched in this session
            if company_name in results:
                result = results[company_name]
                
                # UPDATE: Apply new research results
                # Column dtypes are now properly set during initialization
                
                enhanced_df.at[index, 'Research_Status'] = result['status']
                enhanced_df.at[index, 'Research_Timestamp'] = result.get('search_timestamp', '')
                enhanced_df.at[index, 'Research_Confidence'] = result.get('confidence_score', 0.0)
                enhanced_df.at[index, 'Business_Description'] = result.get('description', '')
                
                if result['status'] == 'found':
                    contacts = result.get('contacts', [])
                    if contacts:
                        enhanced_df.at[index, 'Primary_Email'] = contacts[0]['email']
                        enhanced_df.at[index, 'Phone_Number'] = contacts[0].get('phone', '')
                    
                    enhanced_df.at[index, 'Website'] = result.get('website', '')
                
                updated_count += 1
                print(f"   🔄 Updated: {company_name} -> {result['status']}")
            else:
                # PRESERVE: Leave existing research data untouched
                existing_status = row.get('Research_Status', 'pending')
                if existing_status in ['found', 'not_found']:
                    preserved_count += 1
        
        print(f"   ✅ Merge complete: {updated_count} updated, {preserved_count} preserved")
        return enhanced_df


# Factory functions for backward compatibility
def create_web_scraper() -> WebScraper:
    """Factory function to create WebScraper instance"""
    return WebScraper()


def perform_dummy_web_search(query: str) -> Dict:
    """
    Legacy dummy search function - replaced with real implementation
    """
    try:
        # Try to use real scraper if available
        scraper = WebScraper()
        if scraper.researcher:
            result = scraper.research_company_contacts(query)
            return {
                'query': query,
                'results': [result],
                'search_timestamp': datetime.now().isoformat(),
                'real_search': True
            }
    except Exception as e:
        pass
    
    # Fallback to dummy
    return {
        'query': query,
        'results': [
            {
                'title': f"Search result for {query}",
                'url': f"https://example.com/search?q={query}",
                'snippet': f"Enhanced timber business research for {query}",
                'description': f"Automated research result for timber/wood business: {query}"
            }
        ],
        'search_timestamp': datetime.now().isoformat(),
        'dummy_flag': True
    }
