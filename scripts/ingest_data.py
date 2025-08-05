import os
import sys
import bz2
import shutil
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

headers = {
    "User-Agent": "Mozilla/5.0"  # Spoof a browser or wget
}

DOWNLOAD_DIR = "./tmp/"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = os.path.join(os.getcwd(), "credentials.json")
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
# client = storage.Client(project='data-expo-pipeline')

BUCKET_NAME = "data_expo_bucket"
CHUNK_SIZE = 16 * 1024 * 1024

BASEURL = "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/"
data_url = {"YGU3TD": 2000, "CI5CEM": 2001, "OWJXH3": 2002, "KM2QOA": 2003, "CCAZGT": 2004, "JTFT25": 2005, "EPIFFT": 2006, "2BHLWK": 2007, "EIR0RA": 2008}
information_url = {"XTPZZY": "airports", "3NOQ6Q": "carriers", "XXSL8A": "plane", "YZWKHN": "variable_descriptions"}


def download_yearly_file(year_hash: str):
    url = BASEURL + year_hash
    bz2_path = os.path.join(DOWNLOAD_DIR, f"{data_url[year_hash]}_data.csv.bz2")
    csv_path = os.path.join(DOWNLOAD_DIR, f"{data_url[year_hash]}_data.csv")

    try:
        print(f"Downloading {url}...")
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            with open(bz2_path, "wb") as out_file:
                out_file.write(response.read())
        print(f"Downloaded: {bz2_path}")

        print(f"Unzipping {bz2_path}...")
        with bz2.open(bz2_path, 'rb') as file_in:
            with open(csv_path, 'wb') as file_out:
                shutil.copyfileobj(file_in, file_out)

        print("Unzip completed")

        print("Removing redundant gz files...")
        os.remove(bz2_path)
        print("gz files removed!")

        return csv_path

    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def download_information_file(hash: str):
    url = BASEURL + hash
    csv_path = os.path.join(DOWNLOAD_DIR, f"{information_url[hash]}_data.csv")
    try:
        print(f"Downloading {url}...")
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            with open(csv_path, "wb") as out_file:
                out_file.write(response.read())
        print(f"Downloaded: {csv_path}")

        return csv_path

    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def create_bucket(bucket_name: str):
    """
    Create a bucket if not exists. 
    Only proceeds if the bucket exists and belong to the current project.

    Parameters
    ----------
    bucket_name: str
        Name of the bucket to construct a bucket object.

    Returns
    ----------
    bucket: google.cloud.storage.bucket.Bucket
        A bucket matching the name provided.
    """

    try:
        bucket = client.get_bucket(bucket_name)      
        bucket_ids = [bckt.id for bckt in client.list_buckets()]        

        # Verify that the bucket exists in the current project
        if bucket_name in bucket_ids:
            print(f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding...")
            return bucket
        else:
            print(f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project.")
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
        return bucket

    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name.")
        sys.exit(1)


def upload_to_gcs(file_path, bucket, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE 

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if storage.Blob(name=blob_name, bucket=bucket).exists(client):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


def remove_files(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        print(f"Removed directory: {dir_path}")
    else:
        print(f"Directory does not exist: {dir_path}")


with ThreadPoolExecutor(max_workers=4) as executor:
    yearly_files = list(executor.map(download_yearly_file, data_url.keys()))

with ThreadPoolExecutor(max_workers=4) as executor:
    info_files = list(executor.map(download_information_file, information_url.keys()))

all_files = list(filter(None, yearly_files + info_files))

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(upload_to_gcs, path, create_bucket(BUCKET_NAME)) for path in all_files]
    for future in futures:
        future.result()


if __name__ == "__main__":
    
    remove_files("./tmp/")

