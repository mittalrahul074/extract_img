import time
import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("firebase_credentials.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

#delete all documents in the collection where img_url is null||NaN
def delete_invalid_img_urls(collection_name):
    docs = db.collection(collection_name).stream()

    for doc in docs:
        data = doc.to_dict()
        img_url = data.get("img_url")

        # If img_url is None, empty, nan, or does not start with 'h'
        if (
            not img_url or
            not isinstance(img_url, str) or
            not img_url.startswith("h")
        ):
            print(f"Deleting document {doc.id} (invalid img_url: {img_url})")
            db.collection(collection_name).document(doc.id).delete()

    time.sleep(2)
    print("Cleanup completed.")

def delete_test(collection_name):
    # 1. Delete docs where document ID ends with "_11"
    docs = db.collection(collection_name).stream()
    for doc in docs:
        if doc.id.endswith("_11"):
            print(f"Deleting document {doc.id} (ID ends with _11)")
            db.collection(collection_name).document(doc.id).delete()

    # 2. Delete docs where 'sku' starts with 'test'
    docs = (
        db.collection(collection_name)
        .where("sku", ">=", "test")
        .where("sku", "<=", "test\uf8ff")
        .stream()
    )
    for doc in docs:
        print(f"Deleting document {doc.id} (sku starts with test)")
        db.collection(collection_name).document(doc.id).delete()

    time.sleep(2)  # Pause to ensure all deletions are processed
    print("Cleanup completed.")

#function to change status from old_status to new_status 
def change_status(collection_name, old_status, new_status):
    docs = db.collection(collection_name).where("status", "==", old_status).stream()
    for doc in docs:
        print(f"Changing status of document {doc.id} from {old_status} to {new_status}")
        db.collection(collection_name).document(doc.id).update({"status": new_status})

    time.sleep(2)  # Pause to ensure all updates are processed
    print("Status change completed.")

#function to change status from old_status to new_status if picked_by is "mayank" the update satus to "validated" and add validated_by "mayank"
def validate_picked_by(collection_name, picker_name="mayank", new_status="validated"):
    docs = (
        db.collection(collection_name)
        .where("picked_by", "==", picker_name)
        .where("status", "==", "picked")
        .where("validated_by", "==", "")
        .stream()
    )
    for doc in docs:
        print(f"Validating document {doc.id} picked by {picker_name}")
        db.collection(collection_name).document(doc.id).update({
            "status": new_status,
            "validated_by": picker_name
        })

    time.sleep(2)  # Pause to ensure all updates are processed
    print("Validation completed.")

# where dispatch_date is 14-02-2025 convet it to 03-12-2025
def update_dispatch_date(collection_name, old_date_str, new_date_str):
    docs = db.collection(collection_name).where("dispatch_date", "==", old_date_str).stream()
    for doc in docs:
        print(f"Updating dispatch_date of document {doc.id} from {old_date_str} to {new_date_str}")
        db.collection(collection_name).document(doc.id).update({"dispatch_date": new_date_str})

    time.sleep(2)  # Pause to ensure all updates are processed
    print("Dispatch date update completed.")

import datetime
import pandas as pd
from firebase_admin import firestore
db = firestore.client()

def get_total_return_group(
    collection_name="orders",
    picker_name="return",
    new_status="validated",
    out_filename="return_validated_orders.xlsx",
):
    try:
        # 1) Build IST datetime for 10 Dec 2025
        ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        ist_dt = datetime.datetime(2025, 12, 10, 0, 0, tzinfo=ist)

        # 2) Convert IST → UTC → naive
        utc_dt = ist_dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)

        # 3) Query Firestore
        docs = (
            db.collection(collection_name)
            .where("picked_by", "==", picker_name)
            .where("status", "==", new_status)
            .where("updated_at", ">", utc_dt)
            .stream()
        )

        # 4) Collect data
        data = []
        for doc in docs:
            d = doc.to_dict()
            d["_id"] = doc.id
            data.append(d)

        if not data:
            print("No matching documents found.")
            return

        df = pd.DataFrame(data)

        # 5) Convert ALL timezone-aware datetimes to timezone-naive
        for col in df.columns:
            if df[col].dtype == "datetime64[ns, UTC]":  # pandas stores tz-aware like this
                df[col] = df[col].dt.tz_convert(None)   # drop timezone
            elif df[col].dtype == "object":
                # Detect Python datetime objects with timezone
                df[col] = df[col].apply(
                    lambda x: x.replace(tzinfo=None) if isinstance(x, datetime.datetime) and x.tzinfo else x
                )

        # 6) Save to Excel
        df.to_excel(out_filename, index=False)
        print(f"Excel file created: {out_filename}")

    except Exception as e:
        print("Error while fetching documents:", repr(e))

#switch case to excute either cleanup_firestore or delete_test based on user input
# action = input("Enter action (cleanup image->1/delete_test->2/change_status->3/update mayank->4/change date->5): ").strip().lower()
# if action == "1":
#     delete_invalid_img_urls("products")
# elif action == "2":
#     delete_test("orders")
# elif action == "3":
#     old_status = input("Enter old status: ").strip()
#     new_status = input("Enter new status: ").strip()
#     change_status("orders", old_status, new_status)
# elif action == "4":
#     validate_picked_by("orders", picker_name="mayank", new_status="validated")
#     validate_picked_by("orders", picker_name="return", new_status="validated")
# elif action == "5":
#     old_date_str = input("Enter old dispatch date (DD-MM-YYYY): ").strip()
#     new_date_str = input("Enter new dispatch date (DD-MM-YYYY): ").strip()
#     update_dispatch_date("orders", old_date_str, new_date_str)
# else:
#     print("Invalid action. Please enter 'cleanup' or 'delete_test'.")

get_total_return_group("orders", picker_name="mayank", new_status="validated")