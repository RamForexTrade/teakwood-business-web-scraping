"""
Business Emailer Module
Comprehensive Email Management System for Business Outreach
Integrated with Streamlit Business Researcher
"""

import smtplib
import ssl
import time
import logging
import json
import pandas as pd
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, List, Optional, Tuple, Any
import asyncio
from dataclasses import dataclass
import re

@dataclass
class EmailTemplate:
    """Email template structure"""
    name: str
    subject: str
    html_body: str
    variables: List[str]

class BusinessEmailer:
    """Advanced Business Email Management System"""
    
    def __init__(self):
        self.smtp_server = None
        self.port = None
        self.email = None
        self.password = None
        self.sender_name = None
        self.is_configured = False
        
        # Email tracking
        self.email_log = {
            'total_sent': 0,
            'total_failed': 0,
            'sent_emails': [],
            'failed_emails': [],
            'campaigns': {}
        }
        
        # Load default templates
        self.templates = self.load_default_templates()
    
    def configure_smtp(self, smtp_server: str, port: int, email: str, password: str, sender_name: str = None):
        """Configure SMTP settings"""
        self.smtp_server = smtp_server
        self.port = port
        self.email = email
        self.password = password
        self.sender_name = sender_name or email
        self.is_configured = True
    
    def test_email_config(self) -> Tuple[bool, str]:
        """Test email configuration with cloud deployment support"""
        if not self.is_configured:
            return False, "Email not configured"

        try:
            # Check if running in cloud environment
            import os
            is_cloud = any(env_var in os.environ for env_var in ['RAILWAY_ENVIRONMENT', 'RAILWAY_PROJECT_ID'])

            if is_cloud:
                # In cloud deployment, skip actual SMTP test due to network restrictions
                # Just validate credentials format
                if '@' in self.email and len(self.password) > 0:
                    return True, "Email configuration saved (Cloud mode - SMTP test skipped due to network restrictions)"
                else:
                    return False, "Invalid email format or empty password"

            # Local environment - perform full SMTP test
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.port, timeout=30) as server:
                server.starttls(context=context)
                server.login(self.email, self.password)
                return True, "Email configuration successful"

        except Exception as e:
            error_msg = str(e)
            if "Network is unreachable" in error_msg or "Errno 101" in error_msg:
                # Network issue in cloud deployment
                if '@' in self.email and len(self.password) > 0:
                    return True, "Email configuration saved (Network test failed but credentials stored - emails will be attempted during campaign)"
                else:
                    return False, "Invalid email format or empty password"
            else:
                return False, f"Configuration test failed: {error_msg}"
    
    def load_default_templates(self) -> Dict[str, EmailTemplate]:
        """Load default email templates with TeakWood Business branding"""
        templates = {}
        
        # Business Introduction Template
        templates['business_intro'] = EmailTemplate(
            name="Business Introduction",
            subject="Partnership Opportunity - {your_company_name} Timber & Wood Products",
            html_body="""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 10px;">
                    <div style="text-align: center; margin-bottom: 20px;">
                        <h1 style="color: #2c5530; margin: 0;">🌳 {your_company_name}</h1>
                        <p style="color: #666; font-style: italic;">Premium Timber & Wood Products</p>
                    </div>
                    
                    <h2 style="color: #2c5530;">Dear {business_name},</h2>
                    
                    <p>Greetings from <strong>{your_company_name}</strong>!</p>
                    
                    <p>We are a reputable timber and wood products company, and during our recent market research, we identified your business 
                    <strong>{business_name}</strong> as a quality supplier and potential partner in the timber and wood products industry.</p>
                    
                    <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0;">
                        <h3 style="color: #2c5530; margin-top: 0;">🎯 Our Requirements:</h3>
                        <ul style="margin: 0;">
                            <li><strong>Products:</strong> {product_requirements}</li>
                            <li><strong>Volume:</strong> {volume_requirements}</li>
                            <li><strong>Timeline:</strong> {timeline_requirements}</li>
                            <li><strong>Quality Standards:</strong> {quality_requirements}</li>
                        </ul>
                    </div>
                    
                    <p>We would be delighted to explore partnership opportunities with your organization. Could we schedule a brief discussion to understand your capabilities and explore mutual business prospects?</p>
                    
                    <div style="background-color: #e9f5e9; padding: 15px; border-radius: 8px; margin: 20px 0;">
                        <h3 style="color: #2c5530; margin-top: 0;">📞 Contact Information:</h3>
                        <p style="margin: 5px 0;"><strong>Name:</strong> {sender_name}</p>
                        <p style="margin: 5px 0;"><strong>Company:</strong> {your_company_name}</p>
                        <p style="margin: 5px 0;"><strong>Email:</strong> {your_email}</p>
                        <p style="margin: 5px 0;"><strong>Phone:</strong> {your_phone}</p>
                    </div>
                    
                    <p>We look forward to your positive response and the possibility of a mutually beneficial business relationship.</p>
                    
                    <p style="margin-top: 30px;">
                        Best regards,<br>
                        <strong>{sender_name}</strong><br>
                        <em>{your_company_name}</em>
                    </p>
                    
                    <hr style="border: none; border-top: 1px solid #ddd; margin: 30px 0;">
                    <p style="font-size: 12px; color: #666; text-align: center;">
                        This email was sent in connection with timber and wood products business inquiries.
                    </p>
                </div>
            </body>
            </html>
            """,
            variables=['business_name', 'your_company_name', 'product_requirements', 'volume_requirements', 
                      'timeline_requirements', 'quality_requirements', 'sender_name', 'your_email', 'your_phone']
        )
        
        # Supply Inquiry Template
        templates['supply_inquiry'] = EmailTemplate(
            name="Supply Inquiry",
            subject="Supply Inquiry - {product_requirements} from {your_company_name}",
            html_body="""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 10px;">
                    <div style="text-align: center; margin-bottom: 20px;">
                        <h1 style="color: #2c5530; margin: 0;">🌳 {your_company_name}</h1>
                        <p style="color: #666; font-style: italic;">Supply Chain Inquiry</p>
                    </div>
                    
                    <h2 style="color: #2c5530;">Dear {business_name} Team,</h2>
                    
                    <p>We hope this message finds you well. <strong>{your_company_name}</strong> is actively seeking reliable suppliers for our upcoming projects.</p>
                    
                    <div style="background-color: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #ffc107;">
                        <h3 style="color: #856404; margin-top: 0;">📋 Specific Requirements:</h3>
                        <ul style="margin: 0;">
                            <li><strong>Product Specifications:</strong> {product_requirements}</li>
                            <li><strong>Quantity Needed:</strong> {volume_requirements}</li>
                            <li><strong>Delivery Timeline:</strong> {timeline_requirements}</li>
                            <li><strong>Quality Standards:</strong> {quality_requirements}</li>
                        </ul>
                    </div>
                    
                    <p>We would appreciate if you could provide us with:</p>
                    <ul>
                        <li>Product catalog and specifications</li>
                        <li>Pricing information</li>
                        <li>Minimum order quantities</li>
                        <li>Delivery terms and conditions</li>
                        <li>Quality certifications</li>
                    </ul>
                    
                    <div style="background-color: #e9f5e9; padding: 15px; border-radius: 8px; margin: 20px 0;">
                        <h3 style="color: #2c5530; margin-top: 0;">📞 Get In Touch:</h3>
                        <p style="margin: 5px 0;"><strong>Contact Person:</strong> {sender_name}</p>
                        <p style="margin: 5px 0;"><strong>Email:</strong> {your_email}</p>
                        <p style="margin: 5px 0;"><strong>Phone:</strong> {your_phone}</p>
                    </div>
                    
                    <p>We are looking forward to establishing a long-term business relationship with reliable partners like yourself.</p>
                    
                    <p style="margin-top: 30px;">
                        Best regards,<br>
                        <strong>{sender_name}</strong><br>
                        <em>Procurement Manager</em><br>
                        <strong>{your_company_name}</strong>
                    </p>
                </div>
            </body>
            </html>
            """,
            variables=['business_name', 'your_company_name', 'product_requirements', 'volume_requirements', 
                      'timeline_requirements', 'quality_requirements', 'sender_name', 'your_email', 'your_phone']
        )
        
        return templates
    
    def get_template_list(self) -> List[str]:
        """Get list of available template names"""
        return list(self.templates.keys())
    
    def get_template(self, template_name: str) -> Optional[EmailTemplate]:
        """Get specific template"""
        return self.templates.get(template_name)
    
    def personalize_email(self, template_name: str, business_data: Dict[str, Any], variables: Dict[str, str]) -> Tuple[str, str]:
        """Personalize email template with business data and variables"""
        template = self.get_template(template_name)
        if not template:
            raise ValueError(f"Template '{template_name}' not found")
        
        # Combine business data and template variables
        all_data = {**business_data, **variables}
        
        # Add default values for missing variables
        default_values = {
            'business_name': all_data.get('business_name', 'Valued Partner'),
            'current_date': datetime.now().strftime("%B %d, %Y"),
            'recipient_name': all_data.get('business_name', 'Sir/Madam'),
            'business_description': all_data.get('business_description', ''),
            'business_address': all_data.get('business_address', 'your location'),
            'products_services': all_data.get('products_services', 'quality products and services'),
            'years_in_business': all_data.get('years_in_business', 'several years')
        }
        
        # Merge all data
        email_data = {**default_values, **all_data}
        
        try:
            # Personalize subject and body
            subject = template.subject.format(**email_data)
            body = template.html_body.format(**email_data)
            return subject, body
        except KeyError as e:
            raise ValueError(f"Missing variable for email personalization: {e}")
    
    def send_email(self, to_email: str, subject: str, html_body: str, attachments: List[str] = None) -> Tuple[bool, str]:
        """Send a single email"""
        if not self.is_configured:
            return False, "Email not configured"
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = f"{self.sender_name} <{self.email}>"
            msg['To'] = to_email
            msg['Subject'] = subject
            
            # Add HTML body
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)
            
            # Add attachments if provided
            if attachments:
                for file_path in attachments:
                    try:
                        with open(file_path, "rb") as attachment:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                        
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {file_path.split("/")[-1]}'
                        )
                        msg.attach(part)
                    except FileNotFoundError:
                        return False, f"Attachment file not found: {file_path}"
            
            # Send email
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.port, timeout=30) as server:
                server.starttls(context=context)
                server.login(self.email, self.password)
                server.send_message(msg)
            
            # Update tracking
            self.email_log['total_sent'] += 1
            self.email_log['sent_emails'].append({
                'to': to_email,
                'subject': subject,
                'timestamp': datetime.now().isoformat()
            })
            
            return True, "Email sent successfully"
            
        except Exception as e:
            error_msg = str(e)

            # Update tracking
            self.email_log['total_failed'] += 1
            self.email_log['failed_emails'].append({
                'to': to_email,
                'subject': subject,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            })

            # Provide more helpful error messages for common cloud deployment issues
            if "Network is unreachable" in error_msg or "Errno 101" in error_msg:
                return False, f"Network error: Unable to reach email server. This may be due to cloud deployment network restrictions."
            elif "Authentication failed" in error_msg or "535" in error_msg:
                return False, f"Authentication failed: Please check your email and app password."
            elif "timeout" in error_msg.lower():
                return False, f"Connection timeout: Email server is not responding."
            else:
                return False, f"Failed to send email: {error_msg}"
    
    def send_personalized_email(self, recipient_email: str, business_data: Dict[str, Any], 
                              template_name: str, variables: Dict[str, str]) -> Tuple[bool, str]:
        """Send personalized email using template"""
        try:
            subject, body = self.personalize_email(template_name, business_data, variables)
            return self.send_email(recipient_email, subject, body)
        except Exception as e:
            return False, f"Failed to personalize and send email: {str(e)}"
    
    def send_bulk_emails(self, recipients: List[Dict[str, Any]], template_name: str, 
                        variables: Dict[str, str], delay_seconds: float = 1.0) -> Dict[str, Any]:
        """Send bulk personalized emails"""
        results = {
            'total': len(recipients),
            'sent': 0,
            'failed': 0,
            'details': []
        }
        
        for recipient in recipients:
            try:
                business_data = recipient.get('business_data', {})
                email = recipient.get('email')
                
                if not email:
                    results['failed'] += 1
                    results['details'].append({
                        'email': 'Unknown',
                        'status': 'failed',
                        'error': 'No email address provided'
                    })
                    continue
                
                success, message = self.send_personalized_email(email, business_data, template_name, variables)
                
                if success:
                    results['sent'] += 1
                    results['details'].append({
                        'email': email,
                        'status': 'sent',
                        'message': message
                    })
                else:
                    results['failed'] += 1
                    results['details'].append({
                        'email': email,
                        'status': 'failed',
                        'error': message
                    })
                
                # Delay between emails
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
                    
            except Exception as e:
                results['failed'] += 1
                results['details'].append({
                    'email': recipient.get('email', 'Unknown'),
                    'status': 'failed',
                    'error': str(e)
                })
        
        return results
    
    def get_email_stats(self) -> Dict[str, Any]:
        """Get email sending statistics"""
        return self.email_log.copy()
    
    def export_email_log(self) -> str:
        """Export email log as JSON string"""
        return json.dumps(self.email_log, indent=2)


def get_email_provider_config(provider: str) -> Dict[str, Any]:
    """Get email provider configuration"""
    configs = {
        'gmail': {
            'smtp_server': 'smtp.gmail.com',
            'port': 587,
            'name': 'Gmail',
            'instructions': 'Use App Password for authentication'
        },
        'outlook': {
            'smtp_server': 'smtp-mail.outlook.com',
            'port': 587,
            'name': 'Outlook/Hotmail',
            'instructions': 'Use regular password or App Password'
        },
        'yahoo': {
            'smtp_server': 'smtp.mail.yahoo.com',
            'port': 587,
            'name': 'Yahoo Mail',
            'instructions': 'Use App Password for authentication'
        }
    }
    
    return configs.get(provider.lower(), {})


# Utility functions for email validation
def validate_email(email: str) -> bool:
    """Validate email address format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def clean_email_list(emails: List[str]) -> List[str]:
    """Clean and validate list of email addresses"""
    cleaned_emails = []
    for email in emails:
        email = email.strip().lower()
        if validate_email(email):
            cleaned_emails.append(email)
    return list(set(cleaned_emails))  # Remove duplicates


def extract_emails_from_text(text: str) -> List[str]:
    """Extract email addresses from text"""
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(pattern, text)
    return clean_email_list(emails)
