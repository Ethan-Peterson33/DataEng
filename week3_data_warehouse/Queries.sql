-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `astral-volt-411512.nytaxi.external_green_tripdata`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://de-zoomcamp-ep/GreenTaxiData/green_tripdata_2022-*.parquet']
);

-- Check yello trip data
SELECT * FROM astral-volt-411512.nytaxi.external_green_tripdata limit 10;

create or replace table astral-volt-411512.nytaxi.green_tripdata_non_patitioned as
SELECT * FROM astral-volt-411512.nytaxi.external_green_tripdata;

-- Question 2

select count(distinct PULocationID ) from  astral-volt-411512.nytaxi.external_green_tripdata;

select count(distinct PULocationID ) from astral-volt-411512.nytaxi.green_tripdata_non_patitioned ;

-- Question 3

select count(*)  from astral-volt-411512.nytaxi.green_tripdata_non_patitioned 
where fare_amount = 0;

--Question 4

SELECT * FROM astral-volt-411512.nytaxi.green_tripdata_non_patitioned limit 10;



create or replace table astral-volt-411512.nytaxi.green_tripdata_patitioned 
Partition by date(lpep_pickup_datetime)
cluster by PUlocationID
as
SELECT * FROM astral-volt-411512.nytaxi.green_tripdata_non_patitioned;


-- Question 5 


select distinct PULocationID from astral-volt-411512.nytaxi.green_tripdata_non_patitioned
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

 select distinct PULocationID from astral-volt-411512.nytaxi.green_tripdata_patitioned 
 where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
