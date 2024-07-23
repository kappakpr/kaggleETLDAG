from google.cloud import storage
import os
from airflow.models import Variable

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

if __name__ == "__main__":
    # your api key, read from airflow variables and setup as OS variables
    os.environ["KAGGLE_USERNAME"] = Variable.get("KAGGLE_USERNAME")
    os.environ["KAGGLE_KEY"] = Variable.get("KAGGLE_KEY")

    # import kaggle itself seems to be executing an authentication step and fails if kaggle credentials not found
    import kaggle
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('ilginkarakas/covtype', path='.', unzip=True)

    # Set your GCS bucket name and destination file name
    bucket_name = 'kaggaledata'
    source_file_name = 'covtype.csv'
    destination_blob_name = 'covtype.csv'

    # Upload the CSV file to GCS
    upload_to_gcs(bucket_name, source_file_name, destination_blob_name)