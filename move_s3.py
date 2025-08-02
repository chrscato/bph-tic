import boto3
import os
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# === Config ===
BUCKET = os.getenv("S3_BUCKET")
CORRECT_PREFIX = os.getenv("S3_PREFIX")  # e.g., 'tic-mrf/'
WRONG_PREFIX = "${S3_PREFIX}/"             # hardcoded old folder to move from

# S3 client
s3 = boto3.client("s3")

def move_s3_folder(bucket, from_prefix, to_prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=from_prefix)

    moved = 0
    for page in pages:
        for obj in page.get("Contents", []):
            old_key = obj["Key"]
            new_key = old_key.replace(from_prefix, to_prefix, 1)

            # Copy to new location
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": old_key},
                Key=new_key
            )

            # Delete original
            s3.delete_object(Bucket=bucket, Key=old_key)

            print(f"Moved: {old_key} → {new_key}")
            moved += 1

    print(f"\n✅ Done. Moved {moved} objects from '{from_prefix}' to '{to_prefix}'.")

if __name__ == "__main__":
    if not BUCKET or not CORRECT_PREFIX:
        print("❌ S3_BUCKET or S3_PREFIX not set in .env")
    else:
        move_s3_folder(BUCKET, WRONG_PREFIX, CORRECT_PREFIX)
