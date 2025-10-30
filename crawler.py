"""
CSV website/contact enricher

- Reads the source CSV
- For each row:
  - Normalizes website URL (adds https:// and www when missing)
  - Fetches the web page and extracts phones/emails using tools.extract_contacts
  - Merges extracted phones with phone_number column from the CSV
  - Applies a verification filter function (stub below you can customize)
  - Writes:
      Phone (first phone if any)
      Additional Phones (remaining phones, semicolon-separated)
      Email (first email if any)
  - Preserves website and address columns in the output
- Saves the enriched CSV alongside the input as <original>-enriched.csv

Notes:
- The verify_phone() function below is a placeholder. Replace its logic as needed.
- The phone_number value read from the CSV may contain dashes; we remove '-' for saving.
- Phone de-duplication is done by digit-only form so different formats of the same number are treated as duplicates.
"""

from __future__ import annotations
import os
import requests
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file


import re
import os
import math
from typing import Callable, Iterable, List, Optional, Tuple
from email_validator import validate_email, EmailNotValidError

import pandas as pd
import requests
from urllib.parse import urlparse, urlunparse

from tools import extract_contacts

print("entering crawler.py")

# ------------ Configuration ------------
# Update this path to the CSV you want to enrich (use forward slashes for portability)
INPUT_CSV = r"output/google_maps_data_Roofing_Companies_in_Austin.csv"
REQUEST_TIMEOUT_SECONDS = 20
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)


# ------------ Verification stub (customize me) ------------
# API_KEY = os.getenv("API_KEY")
# API_URL = "https://phonevalidation.abstractapi.com/v1/?api_key={api_key}&phone={phone}"

API_KEY = os.getenv("API_KEY")
API_URL = "https://api.phone-check.xyz/v1-get-phone-details"

def verify_phone(number: str) -> bool:
    """
    Validate phone number using Phone-Check.xyz API.
    Returns True only if:
      ✅ valid number
      ✅ mobile or mobile-capable
      ❌ not disposable
    """
    if not API_KEY:
        raise ValueError("PHONE_CHECK_API_KEY not set in environment variables")

    # Clean number (digits only)
    number_str = ''.join(filter(str.isdigit, str(number)))
    if not number_str:
        return False

    try:
        params = {
            "phone": number_str,
            "x-api-key": API_KEY
        }

        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        print("Phone API result:", data)  # Debug info if you want logging

        # API fields based on docs
        is_valid = data.get("valid", False)
        line_type = str(data.get("type", "")).lower()
        disposable = data.get("isDisposable", False)

        print(f"Valid: {is_valid}, Line Type: {line_type}, Disposable: {disposable}")

        if not is_valid:
            return False

        if disposable:
            return False

        # Accept only proper mobile categories
        if line_type in ("mobile", "fixed_line_or_mobile"):
            return True

        return False

    except requests.RequestException as e:
        print(f"API request error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


    except requests.RequestException as e:
        print(f"API request error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
# ------------ Helpers ------------

def normalize_url(raw: str) -> Optional[str]:
    """Normalize a website string to a fetchable https URL with optional www.

    Behavior:
    - Adds scheme https:// if missing.
    - Adds "www." for bare domains like example.com; leaves subdomains intact.
    - Returns None if the input cannot yield a valid host.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None

    # Ensure a scheme for parsing
    if not s.startswith(("http://", "https://")):
        s = "https://" + s

    p = urlparse(s)

    netloc = p.netloc
    path = p.path

    # Some inputs come like https://example.com without netloc (rare), fix up
    if not netloc and path:
        # Treat path as the host
        netloc, path = path, ""

    if not netloc:
        return None

    # Add www. only for domains without subdomain: domain.tld (exactly one dot)
    if not netloc.lower().startswith("www.") and netloc.count(".") == 1:
        netloc = "www." + netloc

    normalized = urlunparse(("https", netloc, path or "/", "", "", ""))
    return normalized


def fetch_page_text(url: str) -> str:
    """Fetch page text via HTTP GET with basic headers/timeouts.

    Returns empty string on failure.
    """
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return resp.text or ""
    except Exception:
        return ""


def digits_only(s: str) -> str:
    """Return only the digits from a string."""
    return re.sub(r"\D", "", s or "")


def clean_csv_phone_for_save(s: str) -> str:
    """Remove dashes from CSV phone before saving (per requirement)."""
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    return str(s).replace("-", "").strip()


def dedupe_preserve_order(values: Iterable[str]) -> List[str]:
    """De-duplicate values by their digit-only key, preserving first occurrence order."""
    seen = set()
    result: List[str] = []
    for v in values:
        key = digits_only(v)
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        result.append(v)
    return result


def format_us_phone_e164(value: str) -> Optional[str]:
    """Normalize any US phone-like string to E.164 +1XXXXXXXXXX.

    - If 10 digits: prepend +1
    - If 11 digits starting with 1: add leading + (e.g., 1832... -> +1832...)
    - If already +1XXXXXXXXXX: keep as is
    - Otherwise, return None
    """
    d = digits_only(value)
    if not d:
        return None
    if len(d) == 10:
        return "+1" + d
    if len(d) == 11 and d.startswith("1"):
        return "+" + d
    # In case extracted already has +1 and 10 digits after it
    if value.startswith("+1") and len(d) == 11:
        return value
    return None


# ------------ Main enrichment routine ------------

def enrich_csv(input_csv: str = INPUT_CSV) -> str:
    """Enrich the given CSV and return the output CSV path."""
    df = pd.read_csv(input_csv)

    # Prepare new columns
    phones_col: List[str] = []
    addl_phones_col: List[str] = []
    email_col: List[str] = []

    # Use existing columns if they exist, otherwise create stubs for safe access
    if "website" not in df.columns:
        df["website"] = ""
    if "address" not in df.columns:
        df["address"] = ""
    if "phone_number" not in df.columns:
        df["phone_number"] = ""
    
    for _, row in df.iterrows():
        # --- Gather a list of candidate phones ---
        candidates: List[str] = []

        # 1) From the CSV phone_number column: strip '-' for saving, but only digits for verification
        raw_csv_phone = row.get("phone_number", "")
        csv_phone_for_save = clean_csv_phone_for_save(raw_csv_phone)
        if csv_phone_for_save:
            candidates.append(csv_phone_for_save)

        # 2) From the website content (if available)
        row_website = row.get("website", "")
        norm_url = normalize_url(row_website) if row_website else None

        extracted_emails: List[str] = []
        extracted_phones: List[str] = []

        if norm_url:
            html = fetch_page_text(norm_url)
            if html:
                contacts = extract_contacts(html)
                extracted_emails = contacts.get("emails", []) or []
                extracted_phones = contacts.get("phones", []) or []

        # Add extracted phones to candidates
        candidates.extend(extracted_phones)

        # --- Filter and de-duplicate phones ---
        filtered: List[str] = []
        for phone in dedupe_preserve_order(candidates):
            # Convert to int for verification: use digit-only representation
            d = digits_only(phone)
            if not d:
                continue
            try:
                n = int(d)
            except Exception:
                continue

            if verify_phone(n):
                # Normalize to +1XXXXXXXXXX so all phones include the +1 prefix
                normalized = format_us_phone_e164(phone)
                if normalized:
                    filtered.append(normalized)

        # Decide Phone vs Additional Phones
        if not filtered:
            phones_col.append("")
            addl_phones_col.append("")
        elif len(filtered) == 1:
            phones_col.append(filtered[0])
            addl_phones_col.append("")
        else:
            phones_col.append(filtered[0])
            # Comma-separated additional phones as requested
            addl_phones_col.append(", ".join(filtered[1:]))

        # Emails: validate, dedupe (case-insensitive), and join with comma
        valid_emails: List[str] = []
        seen_emails = set()
        for em in extracted_emails:
            em_str = (em or "").strip()
            if not em_str:
                continue
            try:
                info = validate_email(em_str, check_deliverability=True)
                normalized = info.email.lower()
            except EmailNotValidError:
                continue
            if normalized in seen_emails:
                continue
            seen_emails.add(normalized)
            valid_emails.append(normalized)

        email_col.append(", ".join(valid_emails) if valid_emails else "")

    # Attach new columns
    df["Phone"] = phones_col
    df["Additional Phones"] = addl_phones_col
    df["Email"] = email_col

    # Compute output file path with -enriched suffix
    base, ext = os.path.splitext(input_csv)
    if not ext:
        ext = ".csv"
    output_csv = f"{base}-enriched{ext}"

    # Drop the original phone_number column (now represented by Phone)
    df.drop(columns=["phone_number"], inplace=True, errors="ignore")

    # Save enriched CSV
    df.to_csv(output_csv, index=False)

    return output_csv


if __name__ == "__main__":
    out = enrich_csv(INPUT_CSV)
    print(f"Enriched CSV written to: {out}")
    # print(verify_phone("+1 351-213-7734"))