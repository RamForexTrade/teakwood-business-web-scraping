#!/usr/bin/env python3
"""
Test script to verify email fixes are working
"""

import os
import sys
import logging

# Set up logging to see debug info
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.business_emailer import BusinessEmailer

def test_email_service():
    """Test the email service with the fixes"""
    
    print("🧪 Testing Email Service Fixes")
    print("=" * 50)
    
    # Create emailer instance
    emailer = BusinessEmailer()
    
    # Test Resend configuration
    emailer.configure_smtp(
        smtp_server='resend_api',  # Use Resend API
        port=443,
        email='dominic@winwood.com.my',  # Your sender email
        password='resend_api_token',  # Placeholder for Resend
        sender_name='Winwood Business'
    )
    
    print(f"✅ Email configured: {emailer.is_configured}")
    print(f"✅ Use cloud service: {emailer.use_cloud_service}")
    print(f"✅ SMTP server: {emailer.smtp_server}")
    print(f"✅ Sender: {emailer.sender_name} <{emailer.email}>")

    # Check email service availability
    import os
    resend_key = os.environ.get('RESEND_API_KEY')
    sendgrid_key = os.environ.get('SENDGRID_API_KEY')
    print(f"✅ Resend API Key: {'Available' if resend_key else 'Not Set'}")
    print(f"✅ SendGrid API Key: {'Available' if sendgrid_key else 'Not Set'}")

    # Check Resend SDK availability
    try:
        import resend
        print(f"✅ Resend SDK: Available")
    except ImportError:
        print(f"❌ Resend SDK: Not installed - run 'pip install resend'")

    # Test email content
    test_email = "dominic@winwood.com.my"  # Replace with your test email
    subject = "Test Email - TeakWood Business"
    html_body = """
    <html>
    <body>
        <h2>Test Email from TeakWood Business</h2>
        <p>Hello,</p>
        <p>This is a test email to verify that the email service is working correctly after the fixes.</p>
        <p><strong>Resend Email Service Test - Features:</strong></p>
        <ul>
            <li>✅ Resend API integration (3,000 emails/month free)</li>
            <li>✅ Modern cloud email service</li>
            <li>✅ Works in all cloud deployments</li>
            <li>✅ No SMTP port restrictions</li>
            <li>✅ Professional email delivery</li>
            <li>✅ Smart fallback system</li>
        </ul>
        <p><strong>If you received this email, Resend is working perfectly! 🎉</strong></p>
        <p>Best regards,<br>TeakWood Business Team</p>
    </body>
    </html>
    """
    
    print(f"\n📧 Attempting to send test email to: {test_email}")
    print(f"📧 Subject: {subject}")
    
    # Send test email
    try:
        success, message = emailer.send_email(test_email, subject, html_body)
        
        if success:
            print(f"✅ SUCCESS: {message}")
            print("🎉 Email sent successfully!")
        else:
            print(f"❌ FAILED: {message}")
            print("💡 Check the logs above for detailed error information")
            
    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)}")
        import traceback
        print(f"📋 Full traceback: {traceback.format_exc()}")
    
    # Show email log
    print(f"\n📊 Email Statistics:")
    print(f"   Total sent: {emailer.email_log['total_sent']}")
    print(f"   Total failed: {emailer.email_log['total_failed']}")
    
    if emailer.email_log['sent_emails']:
        print(f"   Last sent: {emailer.email_log['sent_emails'][-1]}")
    
    if emailer.email_log['failed_emails']:
        print(f"   Last failed: {emailer.email_log['failed_emails'][-1]}")

if __name__ == "__main__":
    test_email_service()
