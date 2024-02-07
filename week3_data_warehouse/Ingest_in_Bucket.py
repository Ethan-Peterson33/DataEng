from google.cloud import storage
import wget

months = ['01','02','03','04','05','06','07','08','09','10','11','12']
bucket_name = 'de-zoomcamp-ep'
destination_folder = 'GreenTaxiData/'

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client.from_service_account_json('keys.json')
    bucket = storage_client.get_bucket(bucket_name)
    
    # Include the folder in the destination_blob_name
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

def main():
    for month in months:
        current_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month}.parquet'
        filename = wget.download(current_file)
        print(current_file, ' Downloaded')
        
        # Include the folder in the destination_blob_name
        destination_blob_name = destination_folder + f'green_tripdata_2022-{month}.parquet'
        
        upload_blob(bucket_name, filename, destination_blob_name)
        print(current_file, ' Uploaded')
    
    print('All Done')

if __name__ == '__main__':
    main()
