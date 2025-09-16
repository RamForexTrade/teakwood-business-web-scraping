# Backup Pages

This directory contains backup copies of pages that were moved to clean up the Streamlit sidebar.

## Files

### Email Outreach Backups
- `email_outreach_backup.py.bak` - General backup of email outreach functionality
- `email_outreach_BACKUP_20250914_154739.py.bak` - Timestamped backup from Sept 14, 2024
- `email_outreach_BEFORE_EMAIL_MAPPING.py.bak` - Version before email mapping feature
- `email_outreach_complex_backup.py.bak` - Complex functionality backup
- `email_outreach_FIXED.py.bak` - Fixed version backup
- `email_outreach_ORIGINAL_BACKUP.py.bak` - Original version backup
- `email_outreach_simulation.py.bak` - Simulation mode backup

### Other Pages
- `analyze.py.bak` - Analysis page (functionality moved to email_outreach.py)
- `map.py.bak` - Map visualization page (replaced by business_research.py)

## Purpose

These files were moved here on 2025-09-14 to clean up the Streamlit sidebar, which automatically shows all `.py` files in the `pages` directory as navigation options. By moving them here and changing the extension to `.py.bak`, they are preserved for reference but don't appear in the sidebar.

## Active Pages

The following pages remain active in the main `pages` directory:
- `upload.py` - Data upload and filtering
- `business_research.py` - Business research and mapping
- `email_outreach.py` - Email outreach functionality

## Recovery

If you need to restore any of these pages:
1. Copy the desired file from this backup directory
2. Rename it back to `.py` extension
3. Move it to the main `pages` directory
4. The page will automatically appear in the Streamlit sidebar
