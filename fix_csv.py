"""
CSV Fixer Script

This script processes existing CSV files (output from combined.py) to:
- Add +1 prefix to all phone numbers
- Remove duplicate phones between Phone and Additional Phones columns
- Split Email column into Email and Additional Emails columns
- Save fixed CSVs to a 'fixed/' folder
"""

import os
import argparse
import pandas as pd
from pathlib import Path


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
    parser = argparse.ArgumentParser(
        description="Fix CSV files by adding +1 to phones and splitting emails."
    )
    parser.add_argument(
        "-f", "--files",
        type=str,
        nargs='+',
        required=True,
        help="Paths to CSV files to fix (space-separated)"
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="fixed",
        help="Output directory for fixed CSVs (default: 'fixed')"
    )
    args = parser.parse_args()
    
    csv_files = args.files
    output_dir = args.output
    
    print(f"Files to process: {len(csv_files)}")
    print(f"Output directory: {output_dir}")
    print("="*60)
    
    fixed_files = []
    errors = []
    
    for csv_file in csv_files:
        if not os.path.exists(csv_file):
            print(f"\n✗ File not found: {csv_file}")
            errors.append(csv_file)
            continue
        
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
