# import phonenumbers
# from phonenumbers import carrier
# from phonenumbers.phonenumberutil import number_type

# number = "04852251101"
# print(carrier._is_mobile(number_type(phonenumbers.parse(number))))
# import phonenumbers
# from phonenumbers import carrier, number_type, PhoneNumberType

# def verify_number(number, region="US"):
#     try:
#         parsed = phonenumbers.parse(number, region)
#         if not phonenumbers.is_valid_number(parsed):
#             return {"valid": False, "reason": "Invalid format"}

#         num_type = number_type(parsed)
#         line_type = {
#             PhoneNumberType.MOBILE: "mobile",
#             PhoneNumberType.FIXED_LINE: "landline",
#             PhoneNumberType.FIXED_LINE_OR_MOBILE: "mixed",
#             PhoneNumberType.TOLL_FREE: "toll_free",
#             PhoneNumberType.VOIP: "voip",
#             PhoneNumberType.UNKNOWN: "unknown"
#         }.get(num_type, "unknown")

#         return {
#             "valid": True,
#             "line_type": line_type,
#             "carrier": carrier.name_for_number(parsed, "en")
#         }

#     except Exception as e:
#         return {"valid": False, "reason": str(e)}

# # Example list (1 mobile + 1 landline)
# numbers = [
#     "+14155552671",  # mobile (San Francisco)
#     "+12125551234"   # landline (New York)
# ]

# for n in numbers:
#     print(f"{n} -> {verify_number(n)}")



import asyncio
import re
import json
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

def extract_contacts(text: str):    
    # --- Email regex (catches all domains, not just gmail) ---
    email_pattern = re.compile(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', re.IGNORECASE)

    # --- Phone regex patterns for US ---
    phone_patterns = [
        # +1 (832) 810-7822 or +1 832-810-7822 or +1-832-810-7822
        re.compile(r'\+1[\s\-\.]?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}'),
        # (832) 810-7822 or 832-810-7822 (must have separators)
        re.compile(r'\(?\d{3}\)?[\s\-\.]+\d{3}[\s\-\.]+\d{4}'),
    ]

    # --- Extract emails ---
    emails = list(set(email_pattern.findall(text)))

    # --- Extract phones ---
    phones = set()
    for pattern in phone_patterns:
        for match in pattern.findall(text):
            num = re.sub(r'\D', '', match)  # remove all non-digits
            if len(num) == 10:  # add +1 if missing
                num = '+1' + num
            elif len(num) == 11 and num.startswith('1'):
                num = '+' + num
            if len(num) == 12 and num.startswith('+1'):
                phones.add(num)

    # --- Return JSON-style dict ---
    result = {
        "phones": sorted(phones),
        "emails": sorted(emails)
    }
    return result


async def fetch_page_text(url: str) -> str:
    """Fetch visible text content from a URL using a headless browser with anti-bot setup.

    Behavior change: If waiting for "networkidle" times out, we proceed and return whatever
    text is available instead of raising/propagating the timeout.
    """
    browser = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage"
                ]
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                locale="en-US"
            )

            page = await context.new_page()

            # --- Stealth tweaks ---
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)

            # Try to wait for network to be idle, but don't fail if it times out
            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
            except PlaywrightTimeoutError:
                # Best-effort fallback: ensure DOM is at least parsed
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=5000)
                except PlaywrightTimeoutError:
                    pass  # proceed with whatever is available

            # Extract all visible text (best-effort)
            try:
                text = await page.evaluate("""
                    () => {
                        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
                        let result = '';
                        while (walker.nextNode()) {
                            const t = walker.currentNode.textContent?.trim?.() ?? '';
                            if (t) result += t + '\n';
                        }
                        return result;
                    }
                """)
            except Exception:
                # Fallback to innerText if evaluation above fails for any reason
                try:
                    text = await page.evaluate("document.body ? document.body.innerText : ''")
                except Exception:
                    text = ""

            return (text or "").strip()

    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        # Ensure the browser is closed even if errors occur
        try:
            if browser:
                await browser.close()
        except Exception:
            pass
    

if __name__ == "__main__":
    url = "https://www.myroofimprovement.com/"
    import requests
    resp = requests.get(url=url).text
    with open("page_text.txt", "w", encoding="utf-8") as f:
        f.write(resp)
    contacts = extract_contacts(resp)
    print(json.dumps(contacts, indent=2))

