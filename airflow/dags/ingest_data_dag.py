from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime
import os
import bz2
import shutil
import urllib.request
import logging
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

headers = {
    "User-Agent": "Mozilla/5.0"  # Spoof a browser or wget
}

DOWNLOAD_DIR = "./tmp/"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
log = logging.getLogger("airflow.task")

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
# client = storage.Client(project='data-expo-pipeline')

BUCKET_NAME = "data_expo_bucket"
CHUNK_SIZE = 16 * 1024 * 1024

BASEURL = "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/"
data_url = {"YGU3TD": 2000, "CI5CEM": 2001, "OWJXH3": 2002, "KM2QOA": 2003, "CCAZGT": 2004, "JTFT25": 2005, "EPIFFT": 2006, "2BHLWK": 2007, "EIR0RA": 2008}
information_url = {"XTPZZY": "airports", "3NOQ6Q": "carriers", "XXSL8A": "plane", "YZWKHN": "variable_descriptions"}

@dag(dag_id="ingest_data_from_harvard", schedule=None, start_date=datetime(2025, 8, 14), catchup=False, tags=["GCS", "ETL", "BigQuery"])
def data_expo_pipeline():

    @task
    def create_bucket(bucket_name: str):
        try:
            bucket = client.get_bucket(bucket_name)      
            bucket_ids = [bckt.id for bckt in client.list_buckets()]        

            # Verify that the bucket exists in the current project
            if bucket_name in bucket_ids:
                log.info(f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding...")
                return bucket_name
        except NotFound:
            client.create_bucket(bucket_name)
            log.info(f"Created bucket '{bucket_name}'")
            return bucket_name
        except Forbidden:
            raise RuntimeError(f"Bucket {bucket_name} exists but is inaccessible. Bucket name is taken. Please try a different bucket name.")
        
    @task
    def download_yearly_file(year_hash: str) -> str:
        url = BASEURL + year_hash
        bz2_path = os.path.join(DOWNLOAD_DIR, f"{data_url[year_hash]}_data.csv.bz2")
        csv_path = os.path.join(DOWNLOAD_DIR, f"{data_url[year_hash]}_data.csv")

        try:
            log.info(f"Downloading {url}...")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                with open(bz2_path, "wb") as out_file:
                    out_file.write(response.read())
            log.info(f"Downloaded: {bz2_path}")

            log.info(f"Unzipping {bz2_path}...")
            with bz2.open(bz2_path, 'rb') as file_in:
                with open(csv_path, 'wb') as file_out:
                    shutil.copyfileobj(file_in, file_out)

            log.info("Unzip completed")

            log.info("Removing redundant gz files...")
            os.remove(bz2_path)
            log.info("gz files removed!")

            return csv_path

        except Exception as e:
            log.error(f"Failed to download {url}: {e}")
            raise
    
    @task
    def download_information_file(hash: str) -> str:
        url = BASEURL + hash
        csv_path = os.path.join(DOWNLOAD_DIR, f"{information_url[hash]}_data.csv")
        try:
            log.info(f"Downloading {url}...")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                with open(csv_path, "wb") as out_file:
                    out_file.write(response.read())
            log.info(f"Downloaded: {csv_path}")

            return csv_path

        except Exception as e:
            log.error(f"Failed to download {url}: {e}")
            raise

    @task
    def upload_to_gcs(file_path: str, bucket_name: str, max_retries: int = 3, delay_time: int = 5):
        try:
            bucket = client.get_bucket(bucket_name)
        except Exception as e:
            log.error(f"Could not access bucket '{bucket_name}': {e}")
            raise

        blob_name = os.path.basename(file_path)
        blob = bucket.blob(blob_name)
        blob.chunk_size = CHUNK_SIZE 

        for attempt in range(max_retries):
            try:
                log.info(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
                blob.upload_from_filename(file_path)
                log.info(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

                if storage.Blob(name=blob_name, bucket=bucket).exists(client):
                    log.info(f"Verification successful for {blob_name}")
                    return
                else:
                    log.warning(f"Verification failed for {blob_name}, retrying...")

            except Exception as e:
                log.error(f"Failed to upload {file_path} to GCS on attempt {attempt}: {e}")

            time.sleep(delay_time)

        raise RuntimeError(f"Giving up on {file_path} after {max_retries} attempts")
    
    @task
    def cleanup():
        if os.path.exists(DOWNLOAD_DIR):
            shutil.rmtree(DOWNLOAD_DIR)

    bucket_task = create_bucket(BUCKET_NAME)

    yearly_paths = download_yearly_file.expand(year_hash=list(data_url.keys()))
    info_paths = download_information_file.expand(hash=list(information_url.keys()))

    upload_yearly = upload_to_gcs.partial(bucket_name=BUCKET_NAME).expand(file_path=yearly_paths)
    upload_info = upload_to_gcs.partial(bucket_name=BUCKET_NAME).expand(file_path=info_paths)

    [upload_yearly, upload_info] >> cleanup()


dag_instance = data_expo_pipeline()