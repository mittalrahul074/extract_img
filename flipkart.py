import pandas as pd
from openpyxl.utils import get_column_letter

# Path to your XLS file
path = "flipkart.xls"

# ...existing code...
def save_df_to_firebase(df):
    import firebase_admin
    from firebase_admin import credentials, firestore
    import re

    def make_safe_id(s):
        if not s or pd.isna(s):
            return None
        return re.sub(r'[/#?[\]. ]+', '_', str(s).strip())

    # Initialize Firebase only once
    if not firebase_admin._apps:
        cred = credentials.Certificate("firebase_credentials.json")
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    collection_name = "products"
    batch = db.batch()
    count = 0

    for _, row in df.iterrows():
        sku = row.get('sku')
        img = row.get('img')

        if pd.isna(sku):
            continue

        safe_sku = str(sku).strip().lower()
        safe_id = make_safe_id(safe_sku)
        if not safe_id:
            continue

        img_url = None
        if not pd.isna(img):
            candidate = str(img).strip()
            if candidate and re.match(r'^[hH]', candidate):   # require first letter 'h' or 'H'
                img_url = candidate
            else:
                img_url = None
                continue

        data = {
            "sku": safe_sku,
            "timestamp": firestore.SERVER_TIMESTAMP,
        }
        if img_url:
            data["img_url"] = img_url.lower()

        doc_ref = db.collection(collection_name).document(safe_id)
        batch.set(doc_ref, data)
        count += 1

        # commit in batches to avoid large writes
        if count % 500 == 0:
            batch.commit()
            batch = db.batch()

    if count % 500 != 0:
        batch.commit()

    print(f"[+] Successfully saved {count} items to Firestore collection '{collection_name}'")
# ...existing code...

def save_df_to_firebase2(df):
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

        # Convert row to dict safely
        data = {
            "sku": sku.lower(),
            "img_url": row.get("img").lower(),
            "timestamp": firestore.SERVER_TIMESTAMP,
        }

        batch.set(doc_ref, data)

    batch.commit()
    print(f"[+] Successfully saved {len(df)} items to Firestore collection '{collection_name}'")

# get the useful columns from the user dynamically

use_col = "G,"
# user_input = input("Enter additional columns to include (e.g., 'S' for image URLs)").strip()
# if user_input:
#     use_col += user_input

header_df = pd.read_excel(
    path,
    sheet_name=2,
    nrows=0
)

# Find the column containing "Main Image URL"
img_col = None
img_col_letter = None

for idx, col in enumerate(header_df.columns, start=1):
    col_clean = str(col).strip().lower()

    if "main image url" in col_clean:
        img_col = col
        img_col_letter = get_column_letter(idx)
        break

if img_col is None:
    raise Exception("Could not find 'Main Image URL' column")

use_col += img_col_letter or ""

print(f"Using columns: {use_col}")

df = pd.read_excel(
    path,
    sheet_name=2,     # 3rd sheet
    usecols=use_col,    # G = sku, S = img
    skiprows=4
)

# Rename columns to match firebase function expectations
df = df.rename(columns={
    df.columns[0]: "sku",   # G -> sku
    df.columns[1]: "img"    # S -> img
})

# Drop completely empty rows (optional)
df = df.dropna(subset=["sku"])

save_df_to_firebase(df)