import json
import os
import time
import re
from datetime import datetime
from playwright.sync_api import sync_playwright

# Load external config
with open("scraper_config.json", "r") as _f:
    _config = json.load(_f)

DIRECTOR_ID = _config["director_id"]
ICS_FEED_URL = _config["ics_feed_url"]
RECLAIM_DIRECTORS = set(_config.get("reclaim_directors", []))

IFPA_URL = f"https://www.ifpapinball.com/directors/view.php?d={DIRECTOR_ID}"

DATA_DIR = "site_data"
IMAGE_DIR = os.path.join(DATA_DIR, "images")
GALLERY_DIR = os.path.join(DATA_DIR, "gallery")

# Warn if the ICS feed has a known expiry date
_expires = _config.get("ics_feed_expires")
if _expires and datetime.strptime(_expires, "%Y-%m-%d").date() < datetime.now().date():
    print(f"[!] WARNING: ICS feed may be expired (ics_feed_expires: {_expires}). Update scraper_config.json.")

# Ensure directories exist
os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(GALLERY_DIR, exist_ok=True)


def download_image(context, url, filename):
    """Downloads an image by spoofing a recognized social media link crawler."""
    try:
        headers = {
            'User-Agent': 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)',
            'Accept': 'image/*,*/*;q=0.8'
        }

        response = context.request.get(url, headers=headers)
        content_type = response.headers.get('content-type', '')

        if response.status == 200 and 'image' in content_type:
            filepath = os.path.join(IMAGE_DIR, filename)
            with open(filepath, 'wb') as f:
                f.write(response.body())
            return f"images/{filename}"
        else:
            print(f"   [!] Blocked by CDN. URL was: {url[:60]}... (Type: {content_type})")

    except Exception as e:
        print(f"   [!] Image download failed: {e}")

    return None


def enrich_event_details(events_data, page, context, file_prefix):
    """Deep-dives into each event URL to scrape details and Facebook images/times."""
    if not events_data:
        return []

    print(f"\nPhase 2 & 3: Fetching deep details, images, and times for {len(events_data)} {file_prefix} events...")

    fields_to_scrape = {
        "event_name": "Event Name",
        "location": "Location",
        "address": "Address",
        "director": "Director",
        "ranking_system": "Ranking System",
        "registration_opens": "Registration Opens",
        "qualifying_format": "Qualifying Format",
        "player_limit": "Player Limit",
        "registration_fee": "Registration Fee",
        "finals_format": "Finals Format"
    }

    for event in events_data:
        print(f"-> Inspecting [{event['status'].upper()}]: {event['title']}")
        try:
            page.goto(event['url'])

            event['facebook_url'] = ""
            event['website'] = ""
            event['description'] = ""
            event['start_time'] = "Check Tournament Website"
            time_found = False

            for json_key, label in fields_to_scrape.items():
                label_element = page.locator(f'span:text-is("{label}"), b:text-is("{label}")').first
                if label_element.count() > 0:
                    parent_col = label_element.locator('xpath=ancestor::div[contains(@class, "col")][1]')
                    if parent_col.count() > 0:
                        raw_text = parent_col.inner_text()
                        cleaned_value = raw_text.replace(label, "").strip()
                        event[json_key] = cleaned_value if cleaned_value else "Check Tournament Website"
                else:
                    event[json_key] = "Check Tournament Website"

            desc_header = page.locator(
                'div.card-header:has-text("Overall"), div.card-header:has-text("Details")').first
            if desc_header.count() > 0:
                parent_card = desc_header.locator('xpath=ancestor::div[contains(@class, "card")][1]')
                desc_body = parent_card.locator('div.card-body')
                if desc_body.count() > 0:
                    event['description'] = desc_body.inner_text().strip()

            fb_link = page.locator('a[href*="facebook.com"], a[href*="fb.me"]').first
            if fb_link.count() > 0:
                event['facebook_url'] = fb_link.get_attribute('href')
            else:
                web_row = page.locator('a:has-text("Website")').first
                if web_row.count() > 0:
                    event['website'] = web_row.get_attribute('href')

            if event['facebook_url']:
                print(f"   -> Checking Facebook for image and time: {event['facebook_url']}")
                try:
                    page.goto(event['facebook_url'])
                    page.wait_for_load_state("domcontentloaded")
                    time.sleep(2)

                    # --- Extract Image ---
                    og_image_locator = page.locator('meta[property="og:image"]')
                    if og_image_locator.count() > 0:
                        img_url = og_image_locator.first.get_attribute('content')
                        if img_url:
                            safe_filename = f"{file_prefix}_{event['id']}.jpg"
                            downloaded_path = download_image(context, img_url, safe_filename)
                            if downloaded_path:
                                event['image'] = downloaded_path
                                print(f"   -> Success! Downloaded valid event banner.")
                    else:
                        print("   -> No open-graph image found on Facebook.")

                    # --- Extract Time ---
                    og_desc_locator = page.locator('meta[property="og:description"]')
                    if og_desc_locator.count() > 0:
                        desc_text = og_desc_locator.first.get_attribute('content')
                        if desc_text:
                            # Improved regex: catches 7 PM, 7:00pm, 12:30 AM without needing "at "
                            time_match = re.search(r'\b(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))\b', desc_text, re.IGNORECASE)
                            if time_match:
                                event['start_time'] = time_match.group(1).upper()
                                time_found = True
                                print(f"   -> Success! Found Start Time on FB: {event['start_time']}")

                except Exception as e:
                    print(f"   [!] Failed to pull image/time from Facebook: {e}")

            # --- Fallback Time Search (If FB fails or there is no FB link) ---
            if not time_found:
                # Scans the IFPA description and registration text for a time
                combined_text = event.get('description', '') + " " + event.get('registration_opens', '')
                time_match = re.search(r'\b(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))\b', combined_text, re.IGNORECASE)
                if time_match:
                    event['start_time'] = time_match.group(1).upper()
                    print(f"   -> Success! Found Start Time in IFPA info: {event['start_time']}")

            time.sleep(1.5)

        except Exception as e:
            print(f"   [!] Error grabbing details for {event['title']}: {e}")

    return events_data


def scrape_director_events(page):
    """Phase 1: Gathers local director events from the IFPA page."""
    upcoming_events = []
    past_events = []
    today = datetime.now().date()

    print(f"\nNavigating to local IFPA Director Page: {IFPA_URL}...")
    page.goto(IFPA_URL)
    page.wait_for_selector('table')

    rows = page.locator('table tbody tr').all()
    print(f"Found {len(rows)} total rows. Filtering dates...")

    for i, row in enumerate(rows):
        try:
            cols = row.locator('td').all()

            if len(cols) >= 4:
                title_element = cols[0].locator('a').first
                date_text = cols[3].inner_text().strip()

                if title_element.count() > 0 and date_text:
                    title = title_element.inner_text().strip()
                    link = title_element.get_attribute('href')

                    try:
                        event_date = datetime.strptime(date_text, "%b %d, %Y").date()
                        if link and link.startswith('/'):
                            link = f"https://www.ifpapinball.com{link}"

                        event_dict = {
                            "title": title,
                            "date": date_text,
                            "url": link,
                            "image": "default-pinball.jpg",
                            "date_obj": event_date
                        }

                        if event_date >= today:
                            event_dict["status"] = "upcoming"
                            upcoming_events.append(event_dict)
                            print(f"-> Found Upcoming Local Event: {title}")
                        else:
                            event_dict["status"] = "past"
                            past_events.append(event_dict)
                    except ValueError:
                        continue
        except Exception as e:
            pass

    past_events.sort(key=lambda x: x['date_obj'], reverse=True)
    past_events = past_events[:10]

    events_data = upcoming_events + past_events
    for idx, event in enumerate(events_data):
        event["id"] = idx
        del event["date_obj"]

    return events_data


def scrape_michigan_ics_feed(context, existing_urls):
    """Downloads ICS feed, filters for Michigan, and excludes already-scraped local events."""
    upcoming_events = []
    past_events = []
    today = datetime.now().date()

    print(f"\nDownloading global ICS feed from: {ICS_FEED_URL}...")
    response = context.request.get(ICS_FEED_URL)
    ics_text = response.text()

    # Unfold ICS lines (ICS files wrap lines with a leading space)
    raw_lines = ics_text.replace('\r\n', '\n').split('\n')
    unfolded_lines = []
    for line in raw_lines:
        if line.startswith(' ') and unfolded_lines:
            unfolded_lines[-1] += line[1:]
        else:
            unfolded_lines.append(line)

    current_event = {}

    for line in unfolded_lines:
        if line == "BEGIN:VEVENT":
            # Assigning the new default image for secondary events here
            current_event = {"image": "other-default.png", "location": "Check Tournament Website"}
        elif line.startswith("SUMMARY:"):
            current_event["title"] = line.replace("SUMMARY:", "", 1).strip()
        elif line.startswith("DTSTART;VALUE=DATE:"):
            date_str = line.split(":")[1].strip()
            try:
                dt = datetime.strptime(date_str, "%Y%m%d").date()
                current_event["date_obj"] = dt
                current_event["date"] = dt.strftime("%b %d, %Y")
            except ValueError:
                pass
        elif line.startswith("LOCATION:"):
            loc = line.replace("LOCATION:", "", 1).strip()
            if loc: current_event["location"] = loc
        elif line.startswith("DESCRIPTION:"):
            desc = line.replace("DESCRIPTION:", "", 1).strip()
            # Extract the IFPA tournament URL
            url_match = re.search(r'(https?://[^\s]+)', desc)
            if url_match:
                current_event["url"] = url_match.group(1)
        elif line == "END:VEVENT":
            # Check if event has required data and is in Michigan
            if "url" in current_event and "date_obj" in current_event and "location" in current_event:
                loc = current_event["location"]

                # Regex looks for exact words "MI" or "Michigan" (ignores "Miami" etc.)
                if re.search(r'\b(MI|Michigan)\b', loc, re.IGNORECASE):
                    # Skip if we already scraped this for the local director list
                    if current_event["url"] not in existing_urls:
                        if current_event["date_obj"] >= today:
                            current_event["status"] = "upcoming"
                            upcoming_events.append(current_event)
                            print(f"-> Found Upcoming Michigan Event: {current_event['title']}")
                        else:
                            current_event["status"] = "past"
                            past_events.append(current_event)

            current_event = {}  # Reset for next event

    past_events.sort(key=lambda x: x['date_obj'], reverse=True)
    past_events = past_events[:10]

    events_data = upcoming_events + past_events
    for idx, event in enumerate(events_data):
        event["id"] = idx
        del event["date_obj"]

    return events_data


def build_gallery_json():
    """Scans the gallery directory and generates a JSON array of image paths."""
    print(f"\nIndexing gallery photos from {GALLERY_DIR}...")
    valid_extensions = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
    gallery_photos = []

    if os.path.exists(GALLERY_DIR):
        for filename in os.listdir(GALLERY_DIR):
            ext = os.path.splitext(filename)[1].lower()
            if ext in valid_extensions:
                gallery_photos.append({
                    "src": f"site_data/gallery/{filename}",
                    "alt": filename
                })

    json_path = os.path.join(DATA_DIR, "gallery.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(gallery_photos, f, indent=4)
    print(f"Success! Indexed {len(gallery_photos)} photos to {json_path}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = context.new_page()
        # Remove the navigator.webdriver flag that flags headless browsers
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page.set_default_timeout(60000)  # 60s — cloud runners can be slower

        # ==========================================
        # 1. SCRAPE LOCAL DIRECTOR EVENTS
        # ==========================================
        local_events = scrape_director_events(page)
        local_events = enrich_event_details(local_events, page, context, file_prefix="event")

        local_json_path = os.path.join(DATA_DIR, "events.json")
        with open(local_json_path, 'w', encoding='utf-8') as f:
            json.dump(local_events, f, indent=4)
        print(f"\nSuccess! Saved {len(local_events)} fully enriched local events to {local_json_path}")

        # Keep track of local URLs so we don't duplicate them in the state-wide feed
        local_urls = set([e['url'] for e in local_events])

        # ==========================================
        # 2. SCRAPE MICHIGAN ICS EVENTS
        # ==========================================
        michigan_events = scrape_michigan_ics_feed(context, local_urls)
        michigan_events = enrich_event_details(michigan_events, page, context, file_prefix="mi_event")

        # Reclaim any Stacey Siegel events that IFPA temporarily de-listed from the
        # director page (happens between event completion and results posting).
        # These appear in the ICS feed as Michigan events but belong in events.json.
        reclaimed = [e for e in michigan_events if e.get("director") in RECLAIM_DIRECTORS]
        michigan_events = [e for e in michigan_events if e.get("director") not in RECLAIM_DIRECTORS]

        if reclaimed:
            print(f"\nReclaiming {len(reclaimed)} event(s) from Michigan feed back into local events (director match):")
            for e in reclaimed:
                print(f"  -> {e['title']} ({e['date']})")
            local_events.extend(reclaimed)
            # Re-sort: all upcoming first, then past descending, cap past at 10
            upcoming_local = [e for e in local_events if e["status"] == "upcoming"]
            past_local = sorted(
                [e for e in local_events if e["status"] == "past"],
                key=lambda x: datetime.strptime(x["date"], "%b %d, %Y"),
                reverse=True
            )[:10]
            local_events = upcoming_local + past_local
            for idx, event in enumerate(local_events):
                event["id"] = idx
            # Re-save events.json with the reclaimed events included
            with open(local_json_path, 'w', encoding='utf-8') as f:
                json.dump(local_events, f, indent=4)
            print(f"Updated {local_json_path} with reclaimed events.")

        # Re-number the remaining Michigan events
        for idx, event in enumerate(michigan_events):
            event["id"] = idx

        mi_json_path = os.path.join(DATA_DIR, "other_womens_events.json")
        with open(mi_json_path, 'w', encoding='utf-8') as f:
            json.dump(michigan_events, f, indent=4)
        print(f"\nSuccess! Saved {len(michigan_events)} fully enriched Michigan events to {mi_json_path}")

        browser.close()

    # ==========================================
    # 3. BUILD PHOTO GALLERY INDEX
    # ==========================================
    build_gallery_json()


if __name__ == "__main__":
    main()