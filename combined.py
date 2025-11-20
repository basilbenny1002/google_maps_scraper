"""
Combined scraper and enricher for roofing companies in a given city.

This script:
- Takes a city name as input
- Searches Google Maps for various roofing-related keywords in that city
- Combines results, removes duplicates by company name (merging phone numbers if different)
- Enriches the data by scraping websites for additional contacts
- Saves the final enriched CSV with the city name
"""

import os
import sys
import argparse
import pandas as pd
from playwright.sync_api import sync_playwright
from dataclasses import dataclass, asdict, field

# Import from existing files
from main import Business, BusinessList, extract_coordinates_from_url
from crawler import enrich_csv

def scrape_for_search(search_for: str, total: int = 1000) -> BusinessList:
    """Scrape Google Maps for a given search term, returning a BusinessList."""
    
    business_list = BusinessList()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Keep visible for debugging, change to True for production
        page = browser.new_page()

        page.goto("https://www.google.com/maps", timeout=60000)
        page.wait_for_timeout(5000)

        print(f"Searching for: {search_for}")

        page.locator('//input[@id="searchboxinput"]').fill(search_for)
        page.wait_for_timeout(3000)

        page.keyboard.press("Enter")
        page.wait_for_timeout(5000)

        # scrolling
        page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')

        previously_counted = 0
        while True:
            page.mouse.wheel(0, 10000)
            page.wait_for_timeout(3000)

            if (
                page.locator(
                    '//a[contains(@href, "https://www.google.com/maps/place")]'
                ).count()
                >= total
            ):
                listings = page.locator(
                    '//a[contains(@href, "https://www.google.com/maps/place")]'
                ).all()[:total]
                print(f"Total Scraped for '{search_for}': {len(listings)}")
                break
            else:
                if (
                    page.locator(
                        '//a[contains(@href, "https://www.google.com/maps/place")]'
                    ).count()
                    == previously_counted
                ):
                    listings = page.locator(
                        '//a[contains(@href, "https://www.google.com/maps/place")]'
                    ).all()
                    print(f"Arrived at all available for '{search_for}'\nTotal Scraped: {len(listings)}")
                    break
                else:
                    previously_counted = page.locator(
                        '//a[contains(@href, "https://www.google.com/maps/place")]'
                    ).count()
                    print(
                        f"Currently Scraped for '{search_for}': ",
                        page.locator(
                            '//a[contains(@href, "https://www.google.com/maps/place")]'
                        ).count(),
                    )

        # scraping
        for listing in listings:
            try:
                listing.click()
                page.wait_for_timeout(5000)

                name_attribute = 'aria-label'
                address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
                website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
                phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
                review_count_xpath = '//button[@jsaction="pane.reviewChart.moreReviews"]//span'
                reviews_average_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]'
                
                business = Business()

                # Business name: prefer aria-label on the anchor; fallback to details title
                name_attr_val = listing.get_attribute(name_attribute)
                if name_attr_val:
                    business.name = name_attr_val
                else:
                    # fallback from details panel
                    title_loc = page.locator('//h1')
                    business.name = title_loc.first.inner_text().strip() if title_loc.count() > 0 else ""
                if page.locator(address_xpath).count() > 0:
                    business.address = page.locator(address_xpath).all()[0].inner_text()
                else:
                    business.address = ""
                if page.locator(website_xpath).count() > 0:
                    business.website = page.locator(website_xpath).all()[0].inner_text()
                else:
                    business.website = ""
                if page.locator(phone_number_xpath).count() > 0:
                    business.phone_number = page.locator(phone_number_xpath).all()[0].inner_text()
                else:
                    business.phone_number = ""
                if page.locator(review_count_xpath).count() > 0:
                    business.reviews_count = int(
                        page.locator(review_count_xpath).inner_text()
                        .split()[0]
                        .replace(',','')
                        .strip()
                    )
                else:
                    business.reviews_count = ""
                    
                if page.locator(reviews_average_xpath).count() > 0:
                    avg_attr = page.locator(reviews_average_xpath).get_attribute(name_attribute)
                    if avg_attr:
                        try:
                            business.reviews_average = float(
                                avg_attr.split()[0].replace(',', '.').strip()
                            )
                        except Exception:
                            business.reviews_average = ""
                    else:
                        business.reviews_average = ""
                else:
                    business.reviews_average = ""
                
                
                business.latitude, business.longitude = extract_coordinates_from_url(page.url)

                business_list.business_list.append(business)
            except Exception as e:
                print(f'Error occurred: {e}')
        
        browser.close()
    
    return business_list

def process_city(city: str, total: int):
    """Process a single city: scrape, dedupe, enrich, and save."""
    
    print(f"\n{'='*60}")
    print(f"Processing city: {city}")
    print(f"{'='*60}\n")
    
    # Hardcoded keywords
    keywords = [
        "roofing company",
        "roofing contractor", 
        "roof repair",
        "roof replacement",
        "roofing services"
    ]

    all_businesses = []

    # Scrape for each keyword
    for keyword in keywords:
        search_for = f"{keyword} in {city}"
        business_list = scrape_for_search(search_for, total)
        all_businesses.extend(business_list.business_list)
        print(f"Collected {len(business_list.business_list)} businesses for '{search_for}'")

    # Create combined BusinessList
    combined_list = BusinessList()
    combined_list.business_list = all_businesses

    # Convert to dataframe
    df = combined_list.dataframe()

    # Deduplicate by name, merging phone numbers if different
    def join_phones(series):
        phones = series.dropna().astype(str).str.strip()
        phones = phones[phones != ''].unique()
        return '; '.join(phones) if len(phones) > 0 else None

    df_deduped = df.groupby('name').agg({
        'address': 'first',
        'website': 'first', 
        'phone_number': join_phones,
        'reviews_count': 'first',
        'reviews_average': 'first',
        'latitude': 'first',
        'longitude': 'first'
    }).reset_index()

    # Save deduped CSV temporarily
    temp_csv = "output/temp_combined.csv"
    os.makedirs("output", exist_ok=True)
    df_deduped.to_csv(temp_csv, index=False)
    print(f"Saved combined deduped data to {temp_csv}")

    # Enrich the CSV
    enriched_path = enrich_csv(temp_csv)
    print(f"Enriched data saved to {enriched_path}")

    # Load the enriched CSV to split and clean
    df_enriched = pd.read_csv(enriched_path, dtype={'Phone': str, 'Additional Phones': str})
    
    # Process Phone columns: ensure +1 prefix and remove duplicates between Phone and Additional Phones
    def add_plus1(phone_str):
        """Add +1 prefix to phone number if not present."""
        if pd.isna(phone_str) or phone_str == '' or str(phone_str).lower() == 'nan':
            return ''
        phone_str = str(phone_str).strip()
        
        # Remove .0 if present (from pandas reading as float)
        if phone_str.endswith('.0'):
            phone_str = phone_str[:-2]
        
        if not phone_str.startswith('+'):
            # Remove any non-digit characters first
            digits = ''.join(filter(str.isdigit, phone_str))
            if len(digits) == 10:
                return '+1' + digits
            elif len(digits) == 11 and digits.startswith('1'):
                return '+' + digits
            elif digits:  # Any other digits, try to format
                return '+1' + digits if len(digits) == 10 else phone_str
        return phone_str
    
    # Add +1 to Phone column
    if 'Phone' in df_enriched.columns:
        df_enriched['Phone'] = df_enriched['Phone'].apply(add_plus1)
    
    # Process Additional Phones: add +1 and remove duplicates with Phone column
    if 'Additional Phones' in df_enriched.columns and 'Phone' in df_enriched.columns:
        def clean_additional_phones(row):
            main_phone = str(row.get('Phone', '')).strip()
            addl_phones = str(row.get('Additional Phones', '')).strip()
            
            if not addl_phones or addl_phones == 'nan':
                return ''
            
            # Split by comma, add +1, and filter out the main phone
            phones = [add_plus1(p.strip()) for p in addl_phones.split(',')]
            phones = [p for p in phones if p and p != main_phone]
            
            return ', '.join(phones) if phones else ''
        
        df_enriched['Additional Phones'] = df_enriched.apply(clean_additional_phones, axis=1)
    
    # Split Email column into Email and Additional Emails
    if 'Email' in df_enriched.columns:
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
        
        df_enriched[['Email', 'Additional Emails']] = df_enriched['Email'].apply(
            lambda x: pd.Series(split_emails(x))
        )
    
    # Remove unwanted columns
    columns_to_remove = ['latitude', 'longitude', 'reviews_count', 'reviews_average']
    df_enriched = df_enriched.drop(columns=[col for col in columns_to_remove if col in df_enriched.columns], errors='ignore')
    
    # Split into chunks of 80 rows
    chunk_size = 100
    total_rows = len(df_enriched)
    num_chunks = (total_rows + chunk_size - 1) // chunk_size  # Ceiling division
    
    city_clean = city.replace(' ', '_')
    
    if num_chunks == 1:
        # Single file, no suffix needed
        final_path = os.path.join("output", f"{city_clean}_enriched.csv")
        df_enriched.to_csv(final_path, index=False)
        print(f"Final enriched file: {final_path}")
    else:
        # Multiple files with _1, _2, etc.
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            df_chunk = df_enriched.iloc[start_idx:end_idx]
            
            final_path = os.path.join("output", f"{city_clean}_enriched_{i+1}.csv")
            df_chunk.to_csv(final_path, index=False)
            print(f"Final enriched file {i+1}/{num_chunks}: {final_path} ({len(df_chunk)} rows)")
    
    # Clean up temp files
    if os.path.exists(temp_csv):
        os.remove(temp_csv)
    if os.path.exists(enriched_path):
        os.remove(enriched_path)


def main():
    parser = argparse.ArgumentParser(description="Scrape and enrich roofing companies data for one or more cities.")
    parser.add_argument("-c", "--city", type=str, required=True, help="City name(s) to search in. Separate multiple cities with commas (e.g., 'Houston Texas, Miami Florida, New York NY')")
    parser.add_argument("-t", "--total", type=int, default=100, help="Maximum number of listings to scrape per keyword (default: 100)")
    args = parser.parse_args()

    # Parse cities - split by comma and strip whitespace
    cities = [city.strip() for city in args.city.split(',') if city.strip()]
    total = args.total

    print(f"Cities to process: {cities}")
    print(f"Max listings per keyword: {total}\n")

    # Process each city sequentially
    for idx, city in enumerate(cities, 1):
        print(f"\n{'#'*60}")
        print(f"City {idx}/{len(cities)}: {city}")
        print(f"{'#'*60}")
        
        try:
            process_city(city, total)
            print(f"\n✓ Successfully completed processing for {city}")
        except Exception as e:
            print(f"\n✗ Error processing {city}: {e}")
            print("Continuing to next city...\n")
    
    print(f"\n{'='*60}")
    print(f"All cities processed!")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()