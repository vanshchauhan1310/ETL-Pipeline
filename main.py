import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from io import StringIO
from datetime import datetime
import traceback
import urllib.parse

# ================= CONFIG =================
BUCKET_NAME = "brazil-ecommerce-vansh"

RAW_FOLDER = "Raw-Folder/"
CLEANED_FOLDER = "Cleaned-Data/"
HISTORICAL_FOLDER = "Historical-Data/"

DB_NAME = "ecommerce_db"
DB_USER = "root"
DB_PASSWORD = "vansh@1310"
DB_HOST = "localhost"

encoded_password = urllib.parse.quote_plus(DB_PASSWORD)

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}/{DB_NAME}"
)

# ================= S3 =================
s3 = boto3.client(
    "s3",
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name="ap-south-1"
)

# ================= DATABASE =================
def create_database():
    temp_engine = create_engine(
        f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}"
    )
    with temp_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))

# ================= S3 HELPERS =================
def get_all_files():
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=RAW_FOLDER):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                files.append(obj["Key"])
    return files

def read_file_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_csv(obj["Body"], low_memory=False)

def save_to_s3(df, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())

def delete_old_history(table):
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=HISTORICAL_FOLDER + table)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
    print("🧹 Old historical deleted")

# ================= CLEANING =================
def clean_data(df):
    print("🧹 Cleaning Started")

    df = df.copy()

    # Step 1: Standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Step 6: Remove duplicates
    df = df.drop_duplicates()

    # Step 7: Convert to string for CDC consistency
    df = df.astype(str)

    return df

# ================= TABLE =================
def create_table_if_not_exists(df, table):
    with engine.connect() as conn:
        cols = [f"{col} TEXT" for col in df.columns]
        query = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(cols)})"
        conn.execute(text(query))

# ================= DB =================
def get_existing_data(table):
    try:
        return pd.read_sql(f"SELECT * FROM {table}", engine)
    except:
        return pd.DataFrame()

def replace_table(df, table):
    df.to_sql(table, engine, if_exists="replace", index=False)

# ================= CDC =================
def apply_cdc(df_old, df_new, table):

    df_old = df_old.copy()
    df_new = df_new.copy()

    # 👉 Assume first column as ID
    key = df_new.columns[0]

    # Ensure same columns
    df_old = df_old[df_new.columns]

    insert_df = df_new[~df_new[key].isin(df_old[key])]

    delete_df = df_old[~df_old[key].isin(df_new[key])]

    merged = pd.merge(df_old, df_new, on=key, suffixes=("_old", "_new"))

    # Compare row values (excluding key)
    compare_cols = [col for col in df_new.columns if col != key]

    update_rows = []

    for col in compare_cols:
        if f"{col}_old" in merged.columns:
            diff = merged[f"{col}_old"] != merged[f"{col}_new"]
            update_rows.append(diff)

    if update_rows:
        condition = update_rows[0]
        for cond in update_rows[1:]:
            condition = condition | cond

        updated = merged[condition]
    else:
        updated = pd.DataFrame()

    update_df = updated[[col for col in updated.columns if "_new" in col]]
    update_df.columns = [col.replace("_new", "") for col in update_df.columns]

    print(f"📊 CDC → Insert: {len(insert_df)}, Update: {len(update_df)}, Delete: {len(delete_df)}")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    insert_df = insert_df.copy()
    delete_df = delete_df.copy()
    update_df = update_df.copy()

    insert_df["operation"] = "insert"
    delete_df["operation"] = "delete"
    update_df["operation"] = "update"

    insert_df["timestamp"] = now
    delete_df["timestamp"] = now
    update_df["timestamp"] = now

    final_df = pd.concat([insert_df, update_df, delete_df])

    delete_old_history(table)

    snapshot = df_new.copy()
    snapshot["operation"] = "latest"
    snapshot["timestamp"] = now

    save_to_s3(snapshot, f"{HISTORICAL_FOLDER}{table}.csv")
    print(f"📁 Latest snapshot saved: {HISTORICAL_FOLDER}{table}.csv")

    replace_table(df_new, table)

    
def run_pipeline():
    print("🚀 PIPELINE STARTED\n")

    create_database()

    files = get_all_files()

    for file in files:
        try:
            print(f"\nProcessing: {file}")

            df = read_file_from_s3(file)
            df = clean_data(df)

            save_to_s3(df, CLEANED_FOLDER + file.split("/")[-1])

            table = file.split("/")[-1].replace(".csv", "")
            print(f"Table: {table}")

            create_table_if_not_exists(df, table)

            df_old = get_existing_data(table)

            if df_old.empty:
                print("🟢 Initial Load")

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df["operation"] = "initial"
                df["timestamp"] = now

                replace_table(df, table)

                delete_old_history(table)
                save_to_s3(df, f"{HISTORICAL_FOLDER}{table}.csv")

            else:
                apply_cdc(df_old, df, table)

        except Exception:
            print(f"❌ Error in: {file}")
            traceback.print_exc()

    print("\n✅ PIPELINE COMPLETED")


# ================= RUN =================
run_pipeline()
