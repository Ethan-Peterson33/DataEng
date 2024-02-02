
import pyarrow as pa
import pyarrow.parquet as pq
import os 


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\epete\\DE_course\\terraform\\keys\\my_creds.json"


bucket_name = "de-zoomcamp-ep"
object_key = "ny_taxi_data.parquet"
project_id = "astral-volt-411512"


table_name = "nyc_taxi_data"

root_path= f'{bucket_name}/{table_name}'


# date column data['lpep_pickup_date']

@data_exporter
def export_data(data, *args, **kwargs):

    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem = gcs
    )

   
