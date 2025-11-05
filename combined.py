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

def main():
    parser = argparse.ArgumentParser(description="Scrape and enrich roofing companies data for a given city.")
    parser.add_argument("-c", "--city", type=str, required=True, help="City name to search in")
    parser.add_argument("-t", "--total", type=int, default=100, help="Maximum number of listings to scrape per keyword (default: 100)")
    args = parser.parse_args()

    city = args.city
    total = args.total

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
    df_enriched = pd.read_csv(enriched_path)
    
    # Remove unwanted columns
    columns_to_remove = ['latitude', 'longitude', 'reviews_count', 'reviews_average']
    df_enriched = df_enriched.drop(columns=[col for col in columns_to_remove if col in df_enriched.columns], errors='ignore')
    
    # Split into chunks of 80 rows
    chunk_size = 80
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

if __name__ == "__main__":
    main()