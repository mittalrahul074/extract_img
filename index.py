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

def scroll_down(driver, container):
    driver.execute_script("arguments[0].scrollTop += arguments[0].clientHeight - 50;", container)
    driver.execute_script("""
    const container = document.querySelector('.MuiTableContainer-root');
    container.scrollBy(0, 600);
    """)
    time.sleep(PAUSE)

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

        text = btn.get_attribute("innerText").strip()
        #debug
        # print("Button text:", text)
        match = re.search(r'\d+',text)
        expected_count = int(match.group()) if match else None
    except Exception as e:
        print("ERROR:", e)
        expected_count = None

    print(f"[*] Expected total rows: {expected_count or 'unknown'}")

    while empty_scrolls < MAX_EMPTY:
        try:
            # rows = container.find_elements(By.XPATH, TABLE_ROW_XPATH)
            rows = driver.execute_script(
                """ const rows = Array.from(document.querySelectorAll("tr.MuiTableRow-root.MuiTableRow-hover"));
                    return rows.map(r => ({
                    suborder_id: r.querySelector('td:nth-child(3)') ? r.querySelector('td:nth-child(3)').innerText.trim() : null,
                    tabindex: r.getAttribute("tabindex"),
                    img : r.querySelector('img') ? (r.querySelector('img').getAttribute('src') || r.querySelector('img').getAttribute('data-src')) : null,
                    sku : (() => { const skuEl = r.querySelector('td:nth-child(4) div div p'); return skuEl ? skuEl.innerText.trim() : null; })()
                    })); """)
            # print(f"[DEBUG] Found {len(rows)} rows on page")
        except Exception as e:
            print(f"[-] Failed to find rows; retrying... Error: {e}")
            time.sleep(1)
            continue

        new_rows = 0

        for row in rows:
            tab_index = row["suborder_id"]
            if not tab_index or tab_index in seen_indexes:
                continue

            seen_indexes.add(tab_index)
            all_data.append(row)
            new_rows += 1
            last_tabindex = tab_index
            # print(f"[DEBUG] Added row with tabindex={tab_index}, sku={row.get('sku')}, img={row.get('img')}")

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
                driver.execute_script("""
                const container = document.querySelector('.MuiTableContainer-root');
                container.scrollBy(0, 600);
                """)
            else:
                driver.execute_script("window.scrollBy(0, -100);")
                time.sleep(0.3)
                driver.execute_script("window.scrollBy(0, window.innerHeight);")
            same_last_seen = 0
            empty_scrolls += 1
            continue

        # try:
        #     using_outer_scroll = False
        #     if not using_outer_scroll and is_scrollable:
        #         print("[DEBUG] Scrolling container (scrollable detected)")
        #         try:
        #             try:
        #                 print("[DEBUG] Attempting container scroll")
        #                 driver.execute_script("arguments[0].scrollTop += arguments[0].clientHeight - 50;", container)
        #             except Exception as e:
        #                 driver.execute_script("""
        #                 const container = document.querySelector('.MuiTableContainer-root');
        #                 container.scrollBy(0, 600);
        #                 """)
        #                 print(f"[!] Container scroll failed; trying alternative scroll method. Error: {e}")
        #         except Exception as e:
        #             print(f"[!] Container scroll failed; trying outer scroll instead. Error: {e}")
        #             driver.execute_script("window.scrollBy(0, window.innerHeight - 50);")
        #     else:
        #         print("[DEBUG] Scrolling window (fallback to outer scroll)")
        #         try:
        #             print("[DEBUG] Attempting outer scroll (window.scrollBy)")
        #             driver.execute_script("window.scrollBy(0, window.innerHeight - 50);")
        #         except Exception as e:
        #             print(f"[!] window.scrollBy failed: {e}")
                
        #         try:
        #             print("[DEBUG] Attempting container scrollTop adjustment")
        #             driver.execute_script(
        #                 "arguments[0].scrollTop += arguments[0].clientHeight - 50;", container
        #             )
        #         except Exception as e:
        #             print(f"[!] container scrollTop failed: {e}")
                
        #         try:
        #             print("[DEBUG] Attempting container.scrollBy method")
        #             driver.execute_script("""
        #             const container = document.querySelector('.MuiTableContainer-root');
        #             if (container) {
        #                 container.scrollBy(0, 600);
        #             } else {
        #                 console.warn('Container not found');
        #             }
        #             """)
        #         except Exception as e:
        #             print(f"[!] container.scrollBy failed: {e}")
        # except Exception:
            print("[!] Scroll attempt failed; trying outer scroll instead.")
            driver.execute_script("window.scrollBy(0, window.innerHeight - 50);")
            using_outer_scroll = True

        # --- PROFESSIONAL SCROLL LOGIC ---

        try:
            scroll_result = driver.execute_script("""
            function getScrollableParent(el) {
                while (el) {
                    const style = window.getComputedStyle(el);
                    const overflowY = style.overflowY;
                    if ((overflowY === 'auto' || overflowY === 'scroll') &&
                        el.scrollHeight > el.clientHeight) {
                        return el;
                    }
                    el = el.parentElement;
                }
                return document.scrollingElement;
            }

            const row = document.querySelector('tr.MuiTableRow-root');
            const scrollParent = getScrollableParent(row);

            if (!scrollParent) return { status: "no_scroll_parent" };

            const before = scrollParent.scrollTop;
            scrollParent.scrollBy(0, 900);
            const after = scrollParent.scrollTop;

            return {
                status: "scrolled",
                before: before,
                after: after,
                atBottom: (scrollParent.scrollTop + scrollParent.clientHeight >= scrollParent.scrollHeight - 5)
            };
            """)

            print(f"[DEBUG] Scroll status: {scroll_result}")
            time.sleep(5)

            if scroll_result.get("before") == scroll_result.get("after"):
                print("[!] Scroll position did not change → likely reached bottom.")
                #try outer scroll to confirm
                driver.execute_script("window.scrollBy(0, window.innerHeight - 150);")
                empty_scrolls += 1

            if scroll_result.get("atBottom"):
                print("[✓] Reached bottom of scroll container.")
                empty_scrolls += 1

        except Exception as e:
            print(f"[CRITICAL] Scroll JS execution failed: {e}")
            empty_scrolls += 1

        time.sleep(0.8)

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
