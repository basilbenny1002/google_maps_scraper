"""
CSV Fixer Script

This script processes existing CSV files (output from combined.py) to:
- Add +1 prefix to all phone numbers
- Remove duplicate phones between Phone and Additional Phones columns
- Split Email column into Email and Additional Emails columns
- Save fixed CSVs to a 'fixed/' folder

USAGE:
1. Set INPUT_FOLDER below to the folder containing your CSV files
2. Run: python fix_csv.py
3. Fixed CSVs will be saved to 'fixed/' folder
"""

import os
import pandas as pd
from pathlib import Path

# ============================================================
# CONFIGURATION - Change these values as needed
# ============================================================
INPUT_FOLDER = "output"  # Folder containing CSV files to fix
OUTPUT_FOLDER = "fixed"  # Folder where fixed CSVs will be saved
# ============================================================


def add_plus1(phone_str):
    """Add +1 prefix to phone number if not present."""
    if pd.isna(phone_str) or phone_str == '':
        return ''
    phone_str = str(phone_str).strip()
    if not phone_str.startswith('+'):
        # Remove any non-digit characters first
        digits = ''.join(filter(str.isdigit, phone_str))
        if len(digits) == 10:
            return '+1' + digits
        elif len(digits) == 11 and digits.startswith('1'):
            return '+' + digits
    return phone_str


def fix_csv(input_path: str, output_dir: str = "fixed") -> str:
    """
    Fix a CSV file by processing phone numbers and emails.
    
    Args:
        input_path: Path to the input CSV file
        output_dir: Directory to save the fixed CSV (default: "fixed")
    
    Returns:
        Path to the fixed CSV file
    """
    print(f"\nProcessing: {input_path}")
    
    # Read the CSV
    df = pd.read_csv(input_path)
    
    # Process Phone columns: ensure +1 prefix and remove duplicates between Phone and Additional Phones
    
    # Add +1 to Phone column
    if 'Phone' in df.columns:
        df['Phone'] = df['Phone'].apply(add_plus1)
        print(f"  ✓ Fixed Phone column ({df['Phone'].notna().sum()} entries)")
    
    # Process Additional Phones: add +1 and remove duplicates with Phone column
    if 'Additional Phones' in df.columns and 'Phone' in df.columns:
        def clean_additional_phones(row):
            main_phone = str(row.get('Phone', '')).strip()
            addl_phones = str(row.get('Additional Phones', '')).strip()
            
            if not addl_phones or addl_phones == 'nan':
                return ''
            
            # Split by comma, add +1, and filter out the main phone
            phones = [add_plus1(p.strip()) for p in addl_phones.split(',')]
            phones = [p for p in phones if p and p != main_phone]
            
            return ', '.join(phones) if phones else ''
        
        df['Additional Phones'] = df.apply(clean_additional_phones, axis=1)
        print(f"  ✓ Fixed Additional Phones column")
    
    # Split Email column into Email and Additional Emails
    if 'Email' in df.columns:
        def split_emails(email_str):
            """Split comma-separated emails into first email and additional emails."""
            if pd.isna(email_str) or email_str == '':
                return '', ''
            
            emails = [e.strip() for e in str(email_str).split(',') if e.strip()]
            if not emails:
                return '', ''
            elif len(emails) == 1:
                return emails[0], ''
            else:
                return emails[0], ', '.join(emails[1:])
        
        # Check if Additional Emails column already exists
        if 'Additional Emails' not in df.columns:
            df[['Email', 'Additional Emails']] = df['Email'].apply(
                lambda x: pd.Series(split_emails(x))
            )
            print(f"  ✓ Split Email into Email and Additional Emails")
        else:
            print(f"  ℹ Email columns already split, skipping")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    input_filename = os.path.basename(input_path)
    output_path = os.path.join(output_dir, input_filename)
    
    # Save the fixed CSV
    df.to_csv(output_path, index=False)
    print(f"  ✓ Saved to: {output_path}")
    
    return output_path


def main():
    input_dir = INPUT_FOLDER
    output_dir = OUTPUT_FOLDER
    
    print("="*60)
    print("CSV FIXER SCRIPT")
    print("="*60)
    
    # Check if directory exists
    if not os.path.exists(input_dir):
        print(f"✗ Directory not found: {input_dir}")
        print(f"  Please update INPUT_FOLDER in the script to a valid directory.")
        return
    
    if not os.path.isdir(input_dir):
        print(f"✗ Path is not a directory: {input_dir}")
        return
    
    # Find all CSV files in the directory
    csv_files = [
        os.path.join(input_dir, f) 
        for f in os.listdir(input_dir) 
        if f.lower().endswith('.csv')
    ]
    
    if not csv_files:
        print(f"✗ No CSV files found in directory: {input_dir}")
        return
    
    print(f"Input directory: {input_dir}")
    print(f"Files to process: {len(csv_files)}")
    print(f"Output directory: {output_dir}")
    print("="*60)
    
    fixed_files = []
    errors = []
    
    for csv_file in csv_files:
        try:
            fixed_path = fix_csv(csv_file, output_dir)
            fixed_files.append(fixed_path)
        except Exception as e:
            print(f"\n✗ Error processing {csv_file}: {e}")
            errors.append(csv_file)
    
    # Summary
    print("\n" + "="*60)
    print(f"Processing complete!")
    print(f"  ✓ Successfully fixed: {len(fixed_files)} files")
    if errors:
        print(f"  ✗ Errors: {len(errors)} files")
        for err_file in errors:
            print(f"    - {err_file}")
    print("="*60)


if __name__ == "__main__":
    main()
