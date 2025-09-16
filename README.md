# Teakwood Business Web Scraping Application

A comprehensive Streamlit-based web application for business research, contact discovery, and email outreach campaigns targeting timber/wood businesses.

## 🚀 Features

### 📊 **Data Management**
- CSV file upload and processing
- Advanced data filtering and validation
- Smart column detection and mapping
- Data persistence across sessions

### 🔍 **AI-Powered Business Research**
- Automated business contact discovery
- Multi-layer search strategy (General, Government, Industry sources)
- AI-powered contact extraction using Groq
- Research results integration with master dataset

### 📧 **Email Outreach Campaigns**
- Professional email template management
- Bulk email sending with rate limiting
- Campaign tracking and status management
- Email delivery status monitoring

### 🎯 **Smart Navigation**
- Intelligent workflow progression
- Context-aware navigation buttons
- Progress tracking across stages

## 🛠️ Technology Stack

- **Frontend**: Streamlit
- **Backend**: Python 3.8+
- **AI/ML**: Groq API for contact extraction
- **Web Scraping**: Tavily API for business research
- **Email**: SMTP integration for campaign delivery
- **Data Processing**: Pandas, NumPy

## 📋 Prerequisites

- Python 3.8 or higher
- Required API keys:
  - Groq API key (for AI contact extraction)
  - Tavily API key (for web research)
  - SMTP credentials (for email sending)

## 🚀 Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd my_streamlit_app
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up API keys**
   - Configure your API keys in the application settings
   - Groq API: Get from [groq.com](https://groq.com)
   - Tavily API: Get from [tavily.com](https://tavily.com)

4. **Run the application**
   ```bash
   streamlit run app.py
   ```

## 📖 Usage

### Step 1: Data Upload
- Upload your CSV file containing business data
- Apply filters if needed
- Validate data quality

### Step 2: Business Research
- Configure AI research settings
- Run automated business contact discovery
- Review and download enhanced data with research results

### Step 3: Email Outreach
- Set up email templates
- Select recipients from researched businesses
- Launch email campaigns with tracking

## 🔧 Configuration

### API Configuration
- **Groq API**: Used for AI-powered contact extraction
- **Tavily API**: Used for web research and business discovery
- **SMTP Settings**: Configure your email server for campaign delivery

### Research Settings
- **Search Layers**: Enable/disable different research sources
- **Rate Limiting**: Configure delays between API calls
- **Batch Size**: Set number of businesses to research per batch

## 📁 Project Structure

```
my_streamlit_app/
├── app.py                 # Main application entry point
├── controllers.py         # Navigation and flow control
├── state_management.py    # Session state management
├── pages/                 # Application pages
│   ├── upload.py         # Data upload and filtering
│   ├── business_research.py  # AI research functionality
│   ├── email_outreach.py # Email campaign management
│   └── ai_chat.py        # AI chat interface
├── services/             # Core services
│   ├── web_scraper.py    # Business research engine
│   ├── compute.py        # Data processing
│   └── email_service.py  # Email delivery
├── utils/                # Utility functions
│   ├── layout.py         # UI components
│   └── data_utils.py     # Data processing utilities
└── requirements.txt      # Python dependencies
```

## 🔄 Recent Updates

### Data Flow Improvements
- ✅ Fixed AI Research Execution data persistence
- ✅ Enhanced data merging between research and email outreach
- ✅ Improved session state management
- ✅ Smart navigation enhancements

### Email Outreach Enhancements
- ✅ Streamlined recipients table editing
- ✅ Enhanced data source prioritization
- ✅ Improved campaign tracking

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is proprietary software developed for Teakwood Business operations.

## 🆘 Support

For support and questions, please contact the development team.

---

**Note**: This application handles sensitive business data. Ensure proper security measures are in place when deploying to production environments.
