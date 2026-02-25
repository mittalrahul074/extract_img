"""
Meesho supplier table scraper (handles virtualized table of 9 rows that update on scroll).

Adjust:
- URL
- LOGIN credentials & selectors (if login is needed)
- TABLE_CONTAINER_XPATH or TABLE_ROW_XPATH to match the actual page structure
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import re
from selenium.webdriver.common.by import By
import time, os
import random
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

PROFILE_DIR = r"C:\Projects\ecommerce-listing-automation\meesho_profile"
URL = "https://supplier.meesho.com/panel/v3/new/fulfillment/yoxpf/orders/ready-to-ship"
URL2 = "https://supplier.meesho.com/panel/v3/new/fulfillment/yoxpf/orders/pending"
TABLE_CONTAINER_XPATH = "//div[contains(@class, 'MuiTableContainer-root')]"
TABLE_ROW_XPATH = ".//tr[contains(@class, 'MuiTableRow-root') and contains(@class, 'MuiTableRow-hover')]"

MAX_EMPTY = 6
PAUSE = 0.2

def start_driver():
    opts = uc.ChromeOptions()
    opts.add_argument(f"--user-data-dir={PROFILE_DIR}")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument("--start-maximized")
    opts.add_argument("--force-device-scale-factor=0.9")
    return uc.Chrome(version_main=144, service=ChromeService(ChromeDriverManager().install()), options=opts)

def extract_from_row(r):
    img_url = None
    sku = None
    try:
        img = r.find_element(By.XPATH, ".//td[1]//img[@src]")
        img_url = img.get_attribute("src")
    except Exception:
        try:
            img = r.find_element(By.XPATH, ".//td[1]//img[@data-src]")
            img_url = img.get_attribute("data-src")
        except Exception:
            try:
                img = r.find_element(By.XPATH, ".//img[@src]")
                img_url = img.get_attribute("src")
            except Exception:
                pass
    try:
        sku_el = r.find_element(By.XPATH, ".//td[4]//div/div/p")
        sku = sku_el.text.strip() if sku_el.text else None
    except Exception:
        pass
    return {"img_url": img_url, "sku": sku}

def scrape_table(driver):
    wait = WebDriverWait(driver, 15)
    results = []
    seen_indexes = set()
    empty_scrolls = 0
    last_tabindex = None
    same_last_seen = 0
    all_data = []

    # wait for table container
    container = wait.until(EC.presence_of_element_located((By.XPATH, TABLE_CONTAINER_XPATH)))

    # detect if table has its own scroll bar
    is_scrollable = driver.execute_script(
        "return arguments[0].scrollHeight > arguments[0].clientHeight;", container
    )

    # detect total expected rows from selected pagination button (if available)
    try:
        btn = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[contains(@class, 'Mui-selected') and contains(@class, 'css-1x0xy1y')]")
        ))
        match = re.search(r'\d+', btn.text.strip())
        expected_count = int(match.group()) if match else None
    except Exception:
        expected_count = None

    print(f"[*] Expected total rows: {expected_count or 'unknown'}")

    while empty_scrolls < MAX_EMPTY:
        try:
            # rows = container.find_elements(By.XPATH, TABLE_ROW_XPATH)
            rows = driver.execute_script(
                """ const rows = Array.from(document.querySelectorAll("tr.MuiTableRow-root.MuiTableRow-hover"));
                    return rows.map(r => ({
                    tabindex: r.getAttribute("tabindex"),
                    img : r.querySelector('img') ? (r.querySelector('img').getAttribute('src') || r.querySelector('img').getAttribute('data-src')) : null,
                    sku : (() => { const skuEl = r.querySelector('td:nth-child(4) div div p'); return skuEl ? skuEl.innerText.trim() : null; })()
                    })); """)
        except Exception:
            print("[-] Failed to find rows; retrying...")
            time.sleep(1)
            continue

        new_rows = 0

        for row in rows:
            tab_index = row["tabindex"]
            if not tab_index or tab_index in seen_indexes:
                continue

            seen_indexes.add(tab_index)
            # item = extract_from_row(row)
            # if not item:
            #     continue

            # results.append(item)
            all_data.append(row)
            new_rows += 1

        if new_rows:
            empty_scrolls = 0
            print(f"[+] Found {new_rows} new rows (Total: {len(all_data)})")

            if expected_count and len(all_data) >= expected_count:
                print("[✓] Reached expected number of items. Stopping.")
                break
        else:
            empty_scrolls += 1
            print(f"[-] No new rows ({empty_scrolls}/{MAX_EMPTY})")

        # check for repeated last row
        try:
            current_last = rows[-1].get_attribute("tabindex")
            if current_last == last_tabindex:
                same_last_seen += 1
            else:
                same_last_seen = 0
                last_tabindex = current_last
        except Exception:
            pass

        if same_last_seen >= 4:
            print("[*] Last row unchanged — end of list reached.")
            if is_scrollable:
                driver.execute_script("arguments[0].scrollTop -= 100;", container)
                time.sleep(0.3)
                driver.execute_script("arguments[0].scrollTop += arguments[0].clientHeight;", container)
            else:
                driver.execute_script("window.scrollBy(0, -100);")
                time.sleep(0.3)
                driver.execute_script("window.scrollBy(0, window.innerHeight);")
            same_last_seen = 0
            empty_scrolls += 1
            continue
        
        try:
            using_outer_scroll = False
            if not using_outer_scroll and is_scrollable:
                driver.execute_script(
                    "arguments[0].scrollTop += arguments[0].clientHeight - 50;", container
                )
            else:
                driver.execute_script("window.scrollBy(0, window.innerHeight - 50);")
                driver.execute_script(
                    "arguments[0].scrollTop += arguments[0].clientHeight - 50;", container
                )
        except Exception:
            print("[!] Scroll attempt failed; trying outer scroll instead.")
            driver.execute_script("window.scrollBy(0, window.innerHeight - 50);")
            using_outer_scroll = True

        time.sleep(PAUSE)

    print(f"[*] Finished scraping {len(all_data)} rows.")
    return all_data

# ...existing code...
def upgrade_img_url(url):
    if not url:
        return None
    s = str(url).strip()
    # require first character to be 'h' or 'H'
    if not re.match(r'^[hH]', s):
        return None
    # adjust width param if present
    return re.sub(r'width=\d+', 'width=480', s)
# ...existing code...

def upgrade_img_url2(url):
    if not url:
        return None
    return re.sub(r'width=\d+', 'width=480', url)

def save_df_to_firebase(df):
    import firebase_admin
    from firebase_admin import credentials, firestore
    import re

    def make_safe_id(s):
        if not s:
            return None
        # Replace unsafe chars but keep original untouched
        return re.sub(r'[/#?[\]. ]+', '_', s.strip())

    # Initialize Firebase only once
    if not firebase_admin._apps:
        cred = credentials.Certificate("firebase_credentials.json")
        firebase_admin.initialize_app(cred)
        print("OK")

    db = firestore.client()

    # Use a new collection → `products` (recommended)
    collection_name = "products"

    batch = db.batch()

    for _, row in df.iterrows():
        sku = row.get('sku')
        if not sku:
            continue  # skip empty SKU rows

        safe_id = make_safe_id(sku)

        doc_ref = db.collection(collection_name).document(safe_id)

        img_url = upgrade_img_url(row.get("img"))
        if img_url is None:
            continue
        # Convert row to dict safely
        data = {
            "sku": sku.lower(),
            "img_url": img_url.lower(),
            "timestamp": firestore.SERVER_TIMESTAMP,
        }

        batch.set(doc_ref, data)

    batch.commit()
    print(f"[+] Successfully saved {len(df)} items to Firestore collection '{collection_name}'")

def main():
    driver1 = start_driver()
    driver1.execute_script("document.body.style.zoom='90%'")

    try:
        driver1.get(URL)
        time.sleep(2)
        # if login required: pause for manual login
        if "login" in driver1.current_url:
            input("Login in browser then press Enter...")
            time.sleep(1)

        data = scrape_table(driver1)
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=["sku"], keep="last")
        df.to_csv("meesho_table_scrape.csv", index=False, encoding="utf-8-sig")
        print(f"[+] saved {len(df)} rows")
        save_df_to_firebase(df)
    finally:
        driver1.quit()

    driver2 = start_driver()
    driver2.execute_script("document.body.style.zoom='90%'")
    try:
        driver2.get(URL2)
        time.sleep(2)
        # if login required: pause for manual login
        if "login" in driver2.current_url:
            input("Login in browser then press Enter...")
            time.sleep(1)

        data = scrape_table(driver2)
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=["sku"], keep="last")
        df.to_csv("meesho_table_scrape.csv", index=False, encoding="utf-8-sig")
        print(f"[+] saved {len(df)} rows")
        save_df_to_firebase(df)
    finally:
        driver2.quit()

if __name__ == "__main__":
    main()
