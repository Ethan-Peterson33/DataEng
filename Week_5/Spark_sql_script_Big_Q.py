
# Only need this for local or VM instance
#import findspark
#findspark.init()

#from the spark dir run this command to create a cluster  ./sbin/start-master.sh
#Get master location from port 8080 

import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser()


parser.add_argument('--input_green',required=True)    
parser.add_argument('--input_yellow',required=True)  
parser.add_argument('--output',required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

#./sbin/start-worker.sh to start worker while in the spark dir

#Neccesary for Job to be exectuted 
bucket = "dataproc-staging-us-east4-780309090765-t26bppcj"
spark.conf.set('temporaryGcsBucket', bucket)



df_green = spark.read.parquet(input_green)

#Rename pickup and dropoff times in green data

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet(input_yellow)

#Rename pickup and dropoff times in yellow data

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]



df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.registerTempTable('trips_data')


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


# Save the data to BigQuery

df_result.write.format('bigquery') \
    .option('table', output) \
    .save()


#spark.stop()
# to kill instance of cluster execute the following scripts  
# ./sbin/stop-master.sh
# ./sbin/stop-worker.sh
